// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
//
// 压力测试：在服务端挂 10k 条空闲连接，然后客户端用
// SO_LINGER {l_onoff=1, l_linger=0} 同时 close()，触发所有 peer 同时发 RST。
//
// 原理：
//   - 正常 close() 走 FIN 四次挥手，内核会把发送缓冲冲完再走 TIME_WAIT。
//   - 把 SO_LINGER 设成 {1, 0}：close() 直接丢弃缓冲，发 RST，跳过 TIME_WAIT。
//     这是制造"对端突然全断"故障场景的最直接手段。
//   - 服务端在 epoll 上几乎同时被唤醒 10k 个 EPOLLIN|EPOLLERR/HUP，
//     一口气把 10k 个 TcpConnection 拖入 HandleClose() 析构路径。
//   - 这条路径上同时在动：Channel 从 Poller 注销、Timer 还挂在 TimerQueue
//     红黑树里、std::any 里的用户上下文要释放、shared_ptr 计数归零触发
//     ~TcpConnection。如果释放顺序写错（比如 Channel 还没 disable 就先
//     释放 Socket fd、或者 Timer 持有 conn 的 shared_ptr 导致循环引用），
//     RST 风暴会立刻把它打出来。
//
// 验证的不变量：
//   1. 每个 TcpConnection 最终都析构（风暴里没有泄漏）。
//   2. 每个 std::any 上下文（SetContext 绑的对象）都析构。
//   3. context_ 在 disconnect 回调之后析构 —— TcpConnection 的成员变量
//      在 dtor body 返回后才按声明逆序析构，所以 close-callback 链一定
//      跑在 ContextTracker 析构之前。共享 seq 计数器能把这个顺序量化。
//   4. RunAfter() 注册的定时器不能强持 TcpConnection（这里用 weak_ptr
//      捕获）。如果哪天有人把 timer lambda 改成按值捕获 shared_ptr，
//      RST 之后连接不会释放，本测试会卡在 context_alive != 0 上。
//   5. 整个风暴期间不 crash 不 hang —— TimerQueue 和 Channel 的级联
//      清理路径必须能扛住瞬时大量析构。

#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/net_utils.h"
#include "runtime/net/tcp_server.h"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <future>
#include <iostream>
#include <string>
#include <sys/resource.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

using namespace std::chrono_literals;
using runtime::net::EventLoop;
using runtime::net::InetAddress;
using runtime::net::TcpServer;

// epoll fd、eventfd、stdio、日志、监听 socket 等都要占用 fd 名额，
// 预留出这部分余量，避免把整个进程的 fd 撑爆触发 accept 失败。
constexpr std::size_t kFdHeadroom = 64;

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

// 客户端和服务端跑在同一个进程里走 loopback，所以每条逻辑连接占 2 个 fd
// （客户端一个、accept 出来的一个）。先把 RLIMIT_NOFILE 顶到硬上限，
// 再按 (budget - headroom) / 2 算出能承载的真实连接数。
std::size_t PlanConnectionCount(std::size_t desired) {
    rlimit rl{};
    if (::getrlimit(RLIMIT_NOFILE, &rl) != 0) {
        return std::min<std::size_t>(desired, 400);
    }
    if (rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max;
        ::setrlimit(RLIMIT_NOFILE, &rl);
    }
    if (::getrlimit(RLIMIT_NOFILE, &rl) != 0) {
        return std::min<std::size_t>(desired, 400);
    }
    const auto budget = static_cast<std::size_t>(rl.rlim_cur);
    if (budget <= kFdHeadroom) return 0;
    const std::size_t cap = (budget - kFdHeadroom) / 2;
    return std::min(desired, cap);
}

// 塞进 std::any 的上下文 payload。析构顺序通过一个全局共享的原子
// 计数器观测：每次 disconnect 回调 fire 和每次 ContextTracker 析构都
// 从同一个 seq_source 取递增序号，最后只要比较两个"最后一次"的序号
// 大小，就能判断真实析构顺序，而不依赖任何 sleep。
struct ContextTracker {
    std::atomic<int>* alive;
    std::atomic<std::uint64_t>* seq_source;
    std::atomic<std::uint64_t>* last_context_seq;

    ContextTracker(std::atomic<int>* a,
                   std::atomic<std::uint64_t>* src,
                   std::atomic<std::uint64_t>* last)
        : alive(a), seq_source(src), last_context_seq(last) {
        alive->fetch_add(1, std::memory_order_relaxed);
    }

    ~ContextTracker() {
        const auto s = seq_source->fetch_add(1, std::memory_order_acq_rel) + 1;
        last_context_seq->store(s, std::memory_order_release);
        alive->fetch_sub(1, std::memory_order_acq_rel);
    }

    ContextTracker(const ContextTracker&) = delete;
    ContextTracker& operator=(const ContextTracker&) = delete;
};

bool TestRstStorm() {
    constexpr std::size_t kDesired = 10'000;
    const std::size_t target_conns = PlanConnectionCount(kDesired);
    if (!Expect(target_conns >= 256,
                "fd budget too small for a meaningful storm (need ulimit -n >= ~640)")) {
        return false;
    }
    std::cerr << "[info] running RST storm with " << target_conns
              << " connections (desired " << kDesired << ")\n";

    const std::uint16_t port = ReserveLoopbackPort();
    if (!Expect(port != 0, "should reserve an ephemeral loopback port")) return false;

    std::atomic<int> connect_events{0};
    std::atomic<int> disconnect_events{0};
    std::atomic<int> context_alive{0};
    std::atomic<int> timers_fired{0};
    std::atomic<int> timers_after_dtor{0};
    std::atomic<int> timers_registered{0};

    // TimerQueue 内部用的是 ObjectPool<Timer, 512>，每个 EventLoop 最多
    // 同时容纳 512 个 timer。4 个 sub-loop 加起来安全水位也远低于 10k。
    // 这里只在前 N 条连接上挂 timer，既能制造 "timer 还挂着、连接就被
    // RST 干掉" 的并发交叠场景，又不会把对象池撑爆触发断言。
    constexpr int kTimerArmBudget = 1024;

    std::atomic<std::uint64_t> seq_source{0};
    std::atomic<std::uint64_t> last_disconnect_seq{0};
    std::atomic<std::uint64_t> last_context_seq{0};

    std::promise<EventLoop*> ready_promise;
    auto ready_future = ready_promise.get_future();

    std::thread server_thread([&] {
        EventLoop loop;
        TcpServer server(&loop, InetAddress(port, "127.0.0.1"), "RstStormServer");
        server.SetThreadNum(4);

        server.SetConnectionCallback([&](const TcpServer::TcpConnectionPtr& conn) {
            if (conn->Connected()) {
                connect_events.fetch_add(1, std::memory_order_relaxed);
                conn->SetContext(std::make_shared<ContextTracker>(
                    &context_alive, &seq_source, &last_context_seq));

                // 故意用 weak_ptr 捕获：如果换成 shared_ptr，TimerQueue
                // 会强持 conn，RST 之后连接不会析构，本测试就会卡死在
                // context_alive != 0 上。这是对"timer 不应延长连接生命
                // 周期"这条约定的硬验证。
                std::weak_ptr<runtime::net::TcpConnection> weak = conn;
                // 60s 的长延时 timer：本测试窗口（~秒级）内绝不应该触发。
                // 它的存在只是为了在 RST 风暴发生时，让"连接析构 + timer
                // 还在 TimerQueue 红黑树里"这两件事真实地并发起来。
                if (timers_registered.fetch_add(1, std::memory_order_relaxed) <
                    kTimerArmBudget) {
                    conn->GetLoop()->RunAfter(
                        60.0, [weak, &timers_fired, &timers_after_dtor] {
                            timers_fired.fetch_add(1, std::memory_order_relaxed);
                            if (weak.expired()) {
                                timers_after_dtor.fetch_add(1, std::memory_order_relaxed);
                            }
                        });
                }
            } else {
                // 断连分支。这里记 seq 的时机，是在最后一个 shared_ptr
                // 还活着的状态（TcpServer::RemoveConnection 通过 bind 把
                // conn 又 hold 一次），所以这个序号严格早于 ~TcpConnection
                // 跑成员析构、释放 std::any 的时刻。
                const auto s = seq_source.fetch_add(1, std::memory_order_acq_rel) + 1;
                last_disconnect_seq.store(s, std::memory_order_release);
                disconnect_events.fetch_add(1, std::memory_order_relaxed);
            }
        });

        server.SetMessageCallback(
            [](const TcpServer::TcpConnectionPtr&, runtime::net::Buffer& buf,
               runtime::time::Timestamp) { buf.RetrieveAll(); });

        server.Start();
        ready_promise.set_value(&loop);
        loop.Loop();
    });

    EventLoop* loop = ready_future.get();

    // 阶段 1：建立 target_conns 条空闲连接。
    // SO_LINGER{1, 0} 是 RST 风暴的关键：close() 不走 FIN，直接发 RST。
    std::vector<int> clients;
    clients.reserve(target_conns);
    sockaddr_in server_addr = runtime::net::MakeIPv4Address("127.0.0.1", port);

    for (std::size_t i = 0; i < target_conns; ++i) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) break;
        if (::connect(fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
            ::close(fd);
            break;
        }
        linger lg{1, 0};
        ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
        clients.push_back(fd);
    }

    const std::size_t opened = clients.size();
    std::cerr << "[info] opened " << opened << " client connections\n";

    // 风暴前要先确认服务端已经把每条连接的 ConnectionCallback 跑完，
    // 否则 connect_events 还没追上来，"建立 → 析构" 的边界就糊在一起，
    // 后面的不变量校验失去意义。
    for (int i = 0; i < 500; ++i) {
        if (connect_events.load(std::memory_order_relaxed) >= static_cast<int>(opened)) break;
        std::this_thread::sleep_for(10ms);
    }

    bool ok = Expect(connect_events.load(std::memory_order_relaxed) ==
                         static_cast<int>(opened),
                     "server should observe a connect callback for every client");
    ok &= Expect(context_alive.load(std::memory_order_relaxed) ==
                     static_cast<int>(opened),
                 "every connection should hold one live ContextTracker");

    // 阶段 2：RST 风暴。l_linger=0 让 close() 走 RST 而不是 FIN，
    // 内核会立刻向对端发 RST 段，服务端在下一次 epoll_wait 一口气
    // 拿到 10k 个 EPOLLIN|EPOLLHUP/ERR。
    for (int fd : clients) ::close(fd);
    clients.clear();

    // 阶段 3：等所有析构路径自然走完。
    // 不能用 sleep 固定时间——10k 条的清理时延不可预测。轮询计数器
    // 等到 disconnect 全部 fire 且 context 全部释放，再做断言。
    for (int i = 0; i < 1000; ++i) {
        if (disconnect_events.load(std::memory_order_relaxed) ==
                static_cast<int>(opened) &&
            context_alive.load(std::memory_order_relaxed) == 0) {
            break;
        }
        std::this_thread::sleep_for(20ms);
    }

    ok &= Expect(disconnect_events.load(std::memory_order_relaxed) ==
                     static_cast<int>(opened),
                 "every connection should fire its disconnect callback");
    ok &= Expect(context_alive.load(std::memory_order_relaxed) == 0,
                 "every std::any context tracker should be destroyed");

    // 析构顺序断言：最后一次 ContextTracker 析构的 seq 必须大于
    // 最后一次 disconnect 回调的 seq。两边从同一个原子计数器取号，
    // 任何顺序倒置都会立刻反映成 last_context_seq < last_disconnect_seq。
    // 背后的语言机制：context_ 是 TcpConnection 的成员，按 C++ 规范
    // 成员在析构函数 body 返回后才按声明逆序析构；而 CloseCallback
    // 是在 HandleClose 里、~TcpConnection 还没被调用之前 fire 的。
    ok &= Expect(last_context_seq.load(std::memory_order_acquire) >
                     last_disconnect_seq.load(std::memory_order_acquire),
                 "context_ must destruct after disconnect callback (member dtor order)");

    // 60s 的 timer 在秒级测试窗口里本来就不该 fire；如果 fire 了，
    // 说明 TimerQueue 在连接关闭路径上误触发了回调。
    // timers_after_dtor 是双保险：万一未来 timer 真的提前跑了，weak_ptr
    // 也必须已 expired，否则就是有强引用泄漏。
    ok &= Expect(timers_fired.load(std::memory_order_relaxed) == 0,
                 "no 60s timer should have fired during the test window");
    ok &= Expect(timers_after_dtor.load(std::memory_order_relaxed) == 0,
                 "no timer callback should observe a live TcpConnection ref");

    loop->Quit();
    server_thread.join();
    return ok;
}

}  // namespace

int main() {
    try {
        if (!TestRstStorm()) return 1;
    } catch (const std::exception& ex) {
        std::cerr << "[FAIL] unexpected exception: " << ex.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "[FAIL] unexpected unknown exception\n";
        return 1;
    }
    std::cout << "[PASS] rst_storm_smoke_test\n";
    return 0;
}
