// examples/demo_bench_gateway.cc
// 只做一件事：把所有请求反向代理到 UPSTREAM_HOST:UPSTREAM_PORT
//
// 对比 nginx 与 该网关的性能差异
// 想了解如何学习/使用 网关层(反向代理层), 移步 examples/demo_gateway.cc
//
// 编译：
//   cmake --build build-tests --target demo_bench_gateway -j$(nproc)
//
// 启动（3个终端）：
//   终端1: 启动一个 HTTP 上游服务监听 9001
//   终端2: UPSTREAM_PORT=9001 IO_THREADS=4 PORT=8080 ./build-tests/examples/demo_bench_gateway
//
// 验证 (终端3)：
//   curl -i http://127.0.0.1:8080/ # 网关 端口 8080
//   curl -i http://127.0.0.1:8088/ # nginx 配置在端口 8088 (先完成下面的配置！！！)
//
// 压测 (nginx 测试 请先读下文写好配置)：
/*   echo "=== [GW]    50c ===" && wrk -t4 -c50  -d15s --latency http://127.0.0.1:8080/
     echo "=== [nginx] 50c ===" && wrk -t4 -c50  -d15s --latency http://127.0.0.1:8088/
     echo "=== [GW]    200c ===" && wrk -t4 -c200 -d15s --latency http://127.0.0.1:8080/
     echo "=== [nginx] 200c ===" && wrk -t4 -c200 -d15s --latency http://127.0.0.1:8088/
*/
/**
   # nginx 一键配置命令
   # 前提: nginx 已安装。检查: nginx -v
   #
   # 注意: 整段 cat...NGINXEOF 必须一次性粘贴，不能逐行执行。
   #       heredoc 结束标记 NGINXEOF 必须顶格（无前导空格）。
   #
   # ① 生成配置
   cat > /tmp/nginx_bench.conf << 'NGINXEOF'
worker_processes 4;
error_log /dev/null;
pid /tmp/nginx_bench.pid;

events {
    worker_connections 4096;
    use epoll;
    multi_accept on;
}

http {
    access_log off;
    keepalive_timeout 65;
    keepalive_requests 10000;

    upstream backend {
        server 127.0.0.1:9001;
        keepalive 64;
    }

    server {
        listen 8088;

        location / {
            proxy_pass http://backend;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
        }
    }
}
NGINXEOF

   # ② 检查配置语法
   nginx -t -c /tmp/nginx_bench.conf

   # ③ 启动
   nginx -c /tmp/nginx_bench.conf

   # ④ 验证
   curl -i http://127.0.0.1:8088/

   # ⑤ 停止
   nginx -s stop -c /tmp/nginx_bench.conf

 */

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "vexo/gateway/gateway_server.h"
#include "vexo/gateway/upstream.h"
#include "vexo/gateway/upstream_peer.h"
#include "vexo/gateway/upstream_registry.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/event_loop_scheduler.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/reactor_connect.h"
#include "vexo/net/reactor_listener.h"

static std::atomic<long long> g_proxied{0};

static void StatsPrinter() {
  long long prev = 0;
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    const long long cur = g_proxied.load(std::memory_order_relaxed);
    std::printf("[gw-stats] rps=%-8lld total=%lld\n", cur - prev, cur);
    std::fflush(stdout);
    prev = cur;
  }
}

int main() {
  // -- 读配置 --
  auto env_int = [](const char* k, int def) -> int {
    const char* v = std::getenv(k);
    return v ? std::atoi(v) : def;
  };
  const int io_threads = env_int("IO_THREADS", 4);
  const uint16_t listen_port = static_cast<uint16_t>(env_int("PORT", 8080));
  const uint16_t upstream_port = static_cast<uint16_t>(env_int("UPSTREAM_PORT", 9001));

  // -- 忽略 SIGPIPE -- （客户端断联时不 Crash)
  std::signal(SIGPIPE, SIG_IGN);

  // 1. 注册上游
  vexo::gateway::UpstreamRegistry reg;
  auto us =
      std::make_shared<vexo::gateway::Upstream>(vexo::gateway::UpstreamConfig{.name = "backend"});
  us->AddPeer(std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{.name = "127.0.0.1:" + std::to_string(upstream_port),
                                        .host = "127.0.0.1",
                                        .port = upstream_port}));
  reg.Add(us);

  // 2. 网关
  vexo::net::EventLoop loop;
  vexo::net::EventLoopScheduler scheduler(&loop);
  vexo::net::ReactorListener listener(&loop, vexo::net::InetAddress(listen_port));
  vexo::net::ReactorConnector connector(&loop);
  vexo::gateway::GatewayServer<vexo::net::ReactorListener, vexo::net::ReactorConnector> gw(
      listener, scheduler, "BenchGateway", reg, connector);
  gw.set_thread_num(io_threads);
  gw.set_pool_config({.max_idle_per_peer = 64});  // 与 nginx keepalive 64 对齐

  // 3. 代理路由
  gw.AddProxyRoute("/", "backend", "round_robin");

  std::thread stats_thr(StatsPrinter);
  stats_thr.detach();

  gw.Start();
  std::printf("BenchGateway listen=%u upstream=127.0.0.1:%u io_threads=%d\n", listen_port,
              upstream_port, io_threads);
  loop.Loop();
  return 0;
}
