// demo_echo_server.cc — 纯 net 层 HTTP/1.1 echo server
//
// 适合: 如果觉得HTTP层写得太重了， 或者只想用项目网络层进行开发学习
// 只引入 vexo_net（TcpServer / Buffer / EventLoop），
// 不引入 vexo_http，目的是单独压测网络层本身。
// 
//
// 手动在 MessageCallback 里做最小 HTTP/1.1 解析：
//   1. 找 \r\n\r\n（header 结束）
//   2. 读 Content-Length（若有）
//   3. 确认 body 到齐后回一个固定 200 OK
//   4. Retrieve 消耗掉本次请求，继续处理下一条（keep-alive）
//
// 环境变量：
//   PORT=8080  IO_THREADS=4  ET_MODE=1（边沿触发）
//
// 编译：
// # 只重新编译 demo_echo_server（增量，秒级）
// cmake --build build-tests --target demo_echo_server -j$(nproc)
// # 全量重新配置 + 编译（改 CMakeLists 时用）
// cmake -B build-tests -S . && cmake --build build-tests -j$(nproc)
//
// 启动服务器
// # 指定 IO 线程数 和 端口， 默认 LT mode
// IO_THREADS=4 PORT=8080 ./build-tests/examples/demo_echo_server
// --
// # 手动指定 ET mode
// IO_THREADS=4 PORT=8080 ET_MODE=1 ./build-tests/examples/demo_echo_server
// # 验证存活, curl, 先启动服务器，然后另起一个终端 
// curl http://127.0.0.1:8080/
// # 清理
// pkill -f demo_echo_server

/**
 * 
   # Round 1：基线，少连接
   wrk -t4 -c50 -d15s --latency http://127.0.0.1:8080/

   # Round 2：加连接数，看吞吐是否下降
   wrk -t4 -c200 -d15s --latency http://127.0.0.1:8080/

   # Round 3：POST 带 body，测 Content-Length 解析路径
   wrk -t4 -c50 -d15s --latency \
   -s /dev/stdin http://127.0.0.1:8080/ <<'LUA'
   wrk.method = "POST"
   wrk.body   = string.rep("x", 256)
   wrk.headers["Content-Length"] = "256"
   LUA

   # Round 4：ET 模式对比（先用 ET_MODE=1 启动服务器再跑）
   wrk -t4 -c50 -d15s --latency http://127.0.0.1:8080/

   # Round 5：极限压测
   wrk -t8 -c500 -d30s --latency http://127.0.0.1:8080/
 */

#include <atomic>
#include <charconv>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string_view>
#include <thread>

#include "vexo/log/logger.h"
#include "vexo/net/buffer.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/tcp_connection.h"
#include "vexo/net/tcp_server.h"
#include "vexo/time/timestamp.h"

using TcpConnectionPtr = std::shared_ptr<vexo::net::TcpConnection>;

static std::atomic<long long> g_requests{0};
static std::atomic<long long> g_conns{0};

// -- 固定响应体(预分配) --
static const std::string kResp = 
  "HTTP/1.1 200 OK\r\n"
  "ContentType: text/plain\r\n"
  "Content-Length: 2\r\n"
  "Connection: keep-alive\r\n"
  "\r\n"
  "OK";


// ── 从 headers 里解析 Content-Length，找不到返回 0 ──────────────
static std::size_t ParseContentLength(std::string_view headers) {
    // headers 范围：Peek() 到 \r\n\r\n 之前
    constexpr std::string_view kKey = "Content-Length: ";
    auto pos = headers.find(kKey);
    if (pos == std::string_view::npos) return 0;
    pos += kKey.size();
    auto end = headers.find("\r\n", pos);
    if (end == std::string_view::npos) end = headers.size();
    std::size_t n = 0;
    std::from_chars(headers.data() + pos, headers.data() + end, n);
    return n;
}


// ── 核心 MessageCallback ─────────────────────────────────────────
//
// 同一个 TcpConnection 在 keep-alive 下会连续收到多个 HTTP 请求，
// 全部放在同一个 Buffer 里——用 while 循环一次性处理完。
static void OnMessage(const TcpConnectionPtr& conn,
                      vexo::net::Buffer& buf,
                      vexo::time::Timestamp) {
    while (true) {
        // 1. 等 header 完整（必须看到 \r\n\r\n）
        const char* header_end = buf.FindCRLFCRLF();
        if (!header_end) return;  // 数据还没到齐，等下次 epoll 触发

        // 2. 解析 Content-Length
        std::string_view headers(buf.Peek(),
                                 static_cast<std::size_t>(header_end - buf.Peek()));
        std::size_t body_len = ParseContentLength(headers);

        // 3. header_end 指向 \r\n\r\n 的第一个 \r，加 4 跳过它
        std::size_t total = static_cast<std::size_t>(header_end - buf.Peek())
                            + 4 + body_len;
        if (buf.readable_bytes() < total) return;  // body 还没到

        // 4. 消耗整个请求，发固定响应
        buf.Retrieve(total);
        conn->Send(kResp);
        g_requests.fetch_add(1, std::memory_order_relaxed);
    }
}

static void StatsPrinter() {
  long long prev = 0;
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    long long cur = g_requests.load(std::memory_order_relaxed);
    long long conns = g_conns.load(std::memory_order_relaxed);
    std::printf("[stats] rps=%-8lld  total=%-10lld  active_conns=%lld\n",
                    cur - prev, cur, conns);
    std::fflush(stdout);
    prev = cur;
  }
}
int main() {
  // -- 读配置 --
  auto env_int = [](const char* k, int def)->int {
    const char* v = std::getenv(k);
    return v ? std::atoi(v) : def;
  };
  const int      io_threads = env_int("IO_THREADS", 4);
  const uint16_t port       = static_cast<uint16_t>(env_int("PORT", 8080));
  const bool     et_mode    = env_int("ET_MODE", 0) != 0;

  // -- 忽略 SIGPIPE -- （客户端断联时不 Crash)
  std::signal(SIGPIPE, SIG_IGN);

  // -- 初始化日志（写到 echo_server.log，可 tail -f 实时查看） --
  vexo::log::Logger::Instance().Init(
      "echo_server",
      vexo::log::LogLevel::INFO,
      /*flush_interval_ms=*/1000,
      /*roll_size=*/10 * 1024 * 1024);

  // -- 构建服务器 --
  vexo::net::EventLoop main_loop;
  vexo::net::TcpServer server(
    &main_loop,
    vexo::net::InetAddress(port),
    "NetEchoServer");
  
    server.set_thread_num(io_threads);
    if (et_mode) server.set_edge_triggered(true);

  server.set_connection_callback([](const TcpConnectionPtr& conn) {
    g_conns.fetch_add(conn->Connected() ? 1 : -1, std::memory_order_relaxed);
  });
  server.set_message_callback(OnMessage);

  // -- 启动统计线程 ---
  std::thread stats_thr(StatsPrinter);
  stats_thr.detach();

  // -- 启动服务 --
  server.Start();
  std::printf("NetEchoServer port=%u io_threads=%d et=%s  (logs: tail -f echo_server.log)\n",
              port, io_threads, et_mode ? "ON" : "OFF");
  std::fflush(stdout);
  main_loop.Loop();
  return 0;
}
