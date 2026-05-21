// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
//
// Slowloris DoS 漏洞验证测试。
//
// 攻击原理：
//   HTTP/1.1 header 段以 "\r\n\r\n" 结尾。HttpContext 是一个纯字节驱动
//   的状态机：parser 处于 ExpectHeaders 状态时，只要 socket 上还有字节
//   慢慢滴进来，就一直等下去。没有任何 deadline / idle timeout。
//
//   攻击者只需：
//     1. TCP connect
//     2. send "GET / HTTP/1.1\r\nHost: x\r\n"
//     3. 每秒 1 字节地 drip 一个不带 ':' 不带 '\r\n' 的字符
//   服务端这条连接就被永久卡在 ExpectHeaders。N 个攻击者 = N 个 fd +
//   N 个 HttpContext + N 个 epoll 注册项被钉死。常见网关 fd 上限就这么
//   被打爆 —— 服务对正常用户不可用，但 CPU 几乎没动，监控看不出问题。
//
// 这个测试做什么：
//   - 启动一个真 HttpServer。
//   - 开 N 条 slowloris 连接，每条 100ms 滴 1 字节，永远不发完 header。
//   - 等待 kAttackDuration 秒。
//   - 检查服务端有没有自己 force-close 这些慢连接（通过 client 侧
//     非阻塞 recv 看是否拿到 EOF）。
//
// 期望结果：
//   - 当前代码：FAIL —— 0 条被关，DoS 漏洞确认。
//   - 修复后（HttpServer 给每条连接挂一个 header-read deadline timer，
//     超时未 GotAll() 就 conn->Shutdown()）：PASS。
//
// 注意：这个测试是有意写成"修了才能过"的契约测试，跟漏洞修复
// 一起 land。在那之前它会留在 CTest 里持续告警。

#include "runtime/http/http_request.h"
#include "runtime/http/http_response.h"
#include "runtime/http/http_server.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/net_utils.h"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <future>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

using namespace std::chrono_literals;

// 攻击规模：开多少条慢连接。
// 这个数远低于 fd 上限，目的是让测试便宜跑完；真实攻击只要拉到
// 服务端 fd 上限就能 deny service。
constexpr std::size_t kSlowlorisConns = 200;

// drip 间隔。慢到 parser 不可能凑出完整 header，但比"每秒 1 字节"
// 快一些，免得测试跑太久。语义上是同一种攻击。
constexpr auto kDripInterval = 100ms;

// 攻击持续多久后判定。任何 sane 的 header-read timeout 都该 < 5s
// （nginx client_header_timeout 默认 60s，但对网关来说太宽松了，
// 一般建议 3-10s）。
constexpr auto kAttackDuration = 5s;

// 至少多少比例的连接被服务端主动关闭，才算"有保护"。
// 留点余量：如果超时是 3s，drip 间隔 100ms，5s 时窗内全部都该被关。
constexpr double kMinKilledRatio = 0.8;

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "[FAIL] " << message << '\n';
        return false;
    }
    return true;
}

std::uint16_t ReserveLoopbackPort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    int on = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return 0;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        return 0;
    }
    const std::uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

int ConnectNonblocking(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in addr = runtime::net::MakeIPv4Address("127.0.0.1", port);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    // 设成非阻塞，方便后面用 recv() 探测对端是否已 close 而不被卡住。
    const int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    return fd;
}

// 返回值：true 表示对端已经关闭这个 socket（recv 拿到 EOF 或 RST），
// false 表示连接还活着 / 还在等更多数据。
bool ServerClosedUs(int fd) {
    char buf[64];
    while (true) {
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n == 0) return true;                 // EOF，对端 FIN
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return false;
            // ECONNRESET / EPIPE 等 —— 也算对端关了我们
            return true;
        }
        // 拿到字节了。可能是 400 错误响应；继续读到 EOF / 没数据。
        // 拿到任何字节都说明服务端在主动回应这条连接（无论是错误响应
        // 还是关闭），都视作"没卡住"。
        return true;
    }
}

bool TestSlowlorisHasHeaderTimeout() {
    const std::uint16_t port = ReserveLoopbackPort();
    if (!Expect(port != 0, "should reserve an ephemeral loopback port")) return false;

    std::promise<runtime::net::EventLoop*> ready_promise;
    auto ready_future = ready_promise.get_future();
    std::atomic<int> requests_served{0};

    std::thread server_thread([&] {
        runtime::net::EventLoop loop;
        runtime::http::HttpServer server(
            &loop, runtime::net::InetAddress(port, "127.0.0.1"), "SlowlorisVictim");
        server.SetThreadNum(2);
        server.Get("/", [&](const runtime::http::HttpRequest&,
                            runtime::http::HttpResponse& resp) {
            requests_served.fetch_add(1, std::memory_order_relaxed);
            resp.SetStatusCode(runtime::http::StatusCode::Ok);
            resp.SetContentType("text/plain");
            resp.SetBody("OK");
        });
        server.Start();
        ready_promise.set_value(&loop);
        loop.Loop();
    });

    runtime::net::EventLoop* loop = ready_future.get();

    // 阶段 1：开 N 条 slowloris 连接，每条先把 request-line 和一个完整
    // 的 Host header 发完，让 parser 进入 ExpectHeaders 状态。
    std::vector<int> attackers;
    attackers.reserve(kSlowlorisConns);
    const std::string prefix = "GET / HTTP/1.1\r\nHost: x\r\nX-Slow: ";
    for (std::size_t i = 0; i < kSlowlorisConns; ++i) {
        const int fd = ConnectNonblocking(port);
        if (fd < 0) break;
        // prefix 一次性发，小到一定不会阻塞。
        if (::send(fd, prefix.data(), prefix.size(), MSG_NOSIGNAL) !=
            static_cast<ssize_t>(prefix.size())) {
            ::close(fd);
            continue;
        }
        attackers.push_back(fd);
    }
    const std::size_t opened = attackers.size();
    std::cerr << "[info] opened " << opened << " slowloris connections\n";

    // 阶段 2：drip。后台线程每 kDripInterval 给每条连接发 1 字节 'a'，
    // 既不形成 ':'（不会让 parser 觉得 header name 结束），也不形成
    // '\r\n'（不会触发行边界）。理论上可以无限滴下去。
    std::atomic<bool> stop_dripping{false};
    std::thread dripper([&] {
        const char one = 'a';
        while (!stop_dripping.load(std::memory_order_relaxed)) {
            for (int fd : attackers) {
                // 失败就忽略 —— 服务端可能已经 close 了，那正是我们想要的。
                ::send(fd, &one, 1, MSG_NOSIGNAL | MSG_DONTWAIT);
            }
            std::this_thread::sleep_for(kDripInterval);
        }
    });

    std::this_thread::sleep_for(kAttackDuration);
    stop_dripping.store(true, std::memory_order_relaxed);
    dripper.join();

    // 阶段 3：清点。统计有多少条被服务端主动关闭。
    std::size_t killed = 0;
    for (int fd : attackers) {
        if (ServerClosedUs(fd)) ++killed;
    }

    const double ratio = opened ? static_cast<double>(killed) / opened : 0.0;
    std::cerr << "[result] slowloris connections: " << opened << '\n';
    std::cerr << "[result]   server-killed:        " << killed
              << "  (" << (ratio * 100.0) << "%)\n";
    std::cerr << "[result]   still stuck:          " << (opened - killed) << '\n';
    std::cerr << "[result]   legitimate requests:  "
              << requests_served.load(std::memory_order_relaxed) << '\n';

    bool ok = true;
    if (ratio < kMinKilledRatio) {
        std::cerr << "\n";
        std::cerr << "[VULN] HttpContext / HttpServer has NO header-read timeout.\n";
        std::cerr << "[VULN] Slowloris DoS is exploitable: " << (opened - killed)
                  << " conns held " << kAttackDuration.count() << "s @ 1 byte/"
                  << kDripInterval.count() << "ms with zero CPU on the attacker.\n";
        std::cerr << "[VULN] Recommended fix: in HttpServer::OnConnection() arm a\n";
        std::cerr << "[VULN]   deadline timer per connection (e.g. RunAfter(5.0, ...))\n";
        std::cerr << "[VULN]   that checks h1ctx.GotAll() and conn->Shutdown() if not.\n";
        std::cerr << "[VULN]   Reset/cancel the timer on each successful request.\n";
        ok = false;
    }

    // 收尾：关掉所有还活着的 attacker socket，停服务器。
    // 关 fd 之后给 sub-loop 一点时间处理 FIN，让 TcpConnection 在它
    // 自己的 EventLoop 线程里走完析构 —— 否则 TcpServer 析构时会从
    // 主线程对仍挂着的 conn 调 ConnectDestroyed，触发线程归属断言。
    for (int fd : attackers) ::close(fd);
    std::this_thread::sleep_for(500ms);
    loop->Quit();
    server_thread.join();
    return ok;
}

}  // namespace

int main() {
    try {
        if (!TestSlowlorisHasHeaderTimeout()) return 1;
    } catch (const std::exception& ex) {
        std::cerr << "[FAIL] unexpected exception: " << ex.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "[FAIL] unexpected unknown exception\n";
        return 1;
    }
    std::cout << "[PASS] slowloris_smoke_test\n";
    return 0;
}
