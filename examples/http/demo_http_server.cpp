// demo_http_server.cpp — runtime_http 层基准测试
// 
// 以下是对比测试， 但是不妨碍学习 和 使用 http层
// 与 demo_echo_server（纯 net 层）在相同条件下对比：
//   GET  /  → "OK"（2 字节）
//   POST /  → "OK"（接收 body 后返回固定响应）
//
// HttpServer 内部产生的额外开销：
//   HttpContext  — 状态机解析器（头部解析、状态转换）
//   Router       — Trie 匹配（静态段 "/" 1个）
//   std::any     — 连接级解析上下文的装箱/拆箱
//   std::function — Handler 调用
//
// 环境变量：
//   PORT=8080  IO_THREADS=4  ET_MODE=1
// 编译:
// cmake --build build-tests --target demo_http_server -j$(nproc)
// 杀掉旧进程
// pkill -f demo_http_server & sleep 0.3
// 确认端口已释放
// ss -lntp | grep 8081 # 无输出

/**
  # ── 启动两台服务器 ──────────────────────────────────────────
  IO_THREADS=4 PORT=8080 ./build-tests/examples/demo_echo_server &   # 纯 net 层
  IO_THREADS=4 PORT=8081 ./build-tests/examples/demo_http_server &   # HTTP 层
  sleep 0.5

  # ── Round 1：GET 4t 50c 15s ─────────────────────────────────
  echo "=== net layer ===" && wrk -t4 -c50 -d15s --latency http://127.0.0.1:8080/
  echo "=== http layer ===" && wrk -t4 -c50 -d15s --latency http://127.0.0.1:8081/

  # ── Round 2：GET 4t 200c 15s ────────────────────────────────
  echo "=== net layer ===" && wrk -t4 -c200 -d15s --latency http://127.0.0.1:8080/
  echo "=== http layer ===" && wrk -t4 -c200 -d15s --latency http://127.0.0.1:8081/

  # ── Round 3：POST 256B body ──────────────────────────────────
  # 1. 创建 Lua 脚本 (直接复制粘贴)
  cat > /tmp/post_bench.lua << 'EOF'
  wrk.method = "POST"
  wrk.body   = string.rep("x", 256)
  wrk.headers["Content-Length"] = "256"
  EOF

  # 2. -s 指定文件跑 (分别跑 8000, 8081)
  wrk -t4 -c50 -d15s --latency -s /tmp/post_bench.lua http://127.0.0.1:8080/
  wrk -t4 -c50 -d15s --latency -s /tmp/post_bench.lua http://127.0.0.1:8081/

  # ── 清理 ────────────────────────────────────────────────────
  pkill -f demo_echo_server; pkill -f demo_http_server
*/

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "runtime/http/http_request.h"
#include "runtime/http/http_response.h"
#include "runtime/http/http_server.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

static std::atomic<long long> g_requests{0};
static std::atomic<long long> g_conns{0};

static void StatsPrinter() {
    long long prev = 0;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        long long cur   = g_requests.load(std::memory_order_relaxed);
        long long conns = g_conns.load(std::memory_order_relaxed);
        std::printf("[stats] rps=%-8lld  total=%-10lld  active_conns=%lld\n",
                    cur - prev, cur, conns);
        std::fflush(stdout);
        prev = cur;
    }
}

int main() {
  auto env_int = [](const char* k, int def) -> int {
    const char* v = std::getenv(k);
    return v ? std::atoi(v) : def;
  };
  const int io_threads = env_int("IO_THREADS", 4);
  const uint16_t port = static_cast<uint16_t>(env_int("PORT", 8080));
  const bool et_mode = env_int("ET_MODE", 0) != 0;

  std::signal(SIGPIPE, SIG_IGN);

  runtime::net::EventLoop main_loop;
  runtime::http::HttpServer server(
    &main_loop,
    runtime::net::InetAddress(port),
    "HttpEchoServer");

  server.SetThreadNum(io_threads);
  server.SetEdgeTriggered(et_mode);

  // GET / - 固定 "OK" 响应（用于 demo_echo_server 基准对比）
  server.Get("/", [](const runtime::http::HttpRequest&,
                   runtime::http::HttpResponse& resp) {
    resp.SetStatusCode(runtime::http::StatusCode::Ok);
    resp.SetContentType("text/plain");
    resp.SetBody("OK");
    g_requests.fetch_add(1, std::memory_order_relaxed);
  });

  // POST / — 接收 body，同样回 "OK"
  server.Post("/", [](const runtime::http::HttpRequest&,
                    runtime::http::HttpResponse& resp) {
    resp.SetStatusCode(runtime::http::StatusCode::Ok);
    resp.SetContentType("text/plain");
    resp.SetBody("OK");
    g_requests.fetch_add(1, std::memory_order_relaxed);
  });

  // GET /api/health — 供 demo_gateway 健康探针使用
  server.Get("/api/health", [](const runtime::http::HttpRequest&,
                               runtime::http::HttpResponse& resp) {
    resp.SetStatusCode(runtime::http::StatusCode::Ok);
    resp.SetContentType("application/json");
    resp.SetBody("{\"status\":\"ok\"}");
  });

  // GET /api/kv — 供 demo_gateway 代理路由使用
  server.Get("/api/kv", [](const runtime::http::HttpRequest&,
                            runtime::http::HttpResponse& resp) {
    resp.SetStatusCode(runtime::http::StatusCode::Ok);
    resp.SetContentType("application/json");
    resp.SetBody("{\"key\":\"demo\",\"value\":\"hello\"}");
  });

  std::thread stats_thr(StatsPrinter);
  stats_thr.detach();

  server.Start();
  std::printf("HttpEchoServer  port=%u  io_threads=%d  et=%s\n",
              port, io_threads, et_mode ? "ON" : "OFF");
  main_loop.Loop();
  return 0;
}