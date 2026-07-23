// examples/demo_bench_gateway_multi.cc
// 多上游网关 benchmark, 与 nginx 同条件对比.
//
// 与 demo_bench_gateway 区别:
//   - 支持多个 upstream peer (env: UPSTREAM_PORTS=9001,9002,9003,9004)
//   - 可选负载均衡算法 (env: LB_ALGO=round_robin|p2c|least_connection|...)
//
// 与 nginx 对比设置: 上游一律打到 nginx 多端口监听返回 ~512B JSON.
//
// 启动顺序:
//   ① 上游 nginx (4 端口, 见 README/nginx_upstream.conf 一并配)
//   ② 我们: UPSTREAM_PORTS=9001,9002,9003,9004 LB_ALGO=round_robin \
//          PORT=8080 ./build-release/examples/demo_bench_gateway_multi
//   ③ nginx 网关: 配置在 /tmp/nginx_gw_multi.conf, listen 8088
//
// 压测:
//   wrk -t4 -c100 -d15s --latency http://127.0.0.1:8080/
//   wrk -t4 -c100 -d15s --latency http://127.0.0.1:8088/

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "vexo/gateway/gateway_server.h"
#include "vexo/gateway/gateway_session_service.h"
#include "vexo/gateway/upstream.h"
#include "vexo/gateway/upstream_peer.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/event_loop_scheduler.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/reactor_connect.h"
#include "vexo/net/reactor_listener.h"
#include "vexo/net/reactor_worker_group.h"

namespace {

std::atomic_bool g_stop{false};

void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

std::vector<uint16_t> ParsePorts(std::string_view csv) {
  std::vector<uint16_t> out;
  std::size_t i = 0;
  while (i < csv.size()) {
    std::size_t j = csv.find(',', i);
    if (j == std::string_view::npos) j = csv.size();
    out.push_back(static_cast<uint16_t>(std::atoi(std::string(csv.substr(i, j - i)).c_str())));
    i = j + 1;
  }
  return out;
}

const char* EnvOr(const char* key, const char* def) {
  const char* v = std::getenv(key);
  return v ? v : def;
}

int EnvInt(const char* key, int def) {
  const char* v = std::getenv(key);
  return v ? std::atoi(v) : def;
}

std::size_t EnvSize(const char* key, std::size_t def) {
  const char* v = std::getenv(key);
  if (!v) return def;
  char* end = nullptr;
  unsigned long long parsed = std::strtoull(v, &end, 10);
  return end != v ? static_cast<std::size_t>(parsed) : def;
}

}  // namespace

int main() {
  const uint16_t listen_port = static_cast<uint16_t>(EnvInt("PORT", 8080));
  const auto ports_csv = std::string(EnvOr("UPSTREAM_PORTS", "9001"));
  const auto algo = std::string(EnvOr("LB_ALGO", "round_robin"));
  const auto max_concurrent = EnvSize("MAX_CONCURRENT_REQUESTS", 1024);
  const auto worker_num = EnvSize("WORKERS", EnvSize("REACTOR_WORKERS", 4));
  const auto max_idle_per_peer = EnvInt("MAX_IDLE_PER_PEER", 0);
  const auto max_idle_total = EnvSize("MAX_IDLE_TOTAL", 64);

  if (worker_num == 0) {
    std::fprintf(stderr, "WORKERS must be greater than zero\n");
    return 1;
  }

  const auto ports = ParsePorts(ports_csv);
  if (ports.empty()) {
    std::fprintf(stderr, "no upstream ports parsed from '%s'\n", ports_csv.c_str());
    return 1;
  }

  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  vexo::gateway::UpstreamRegistry reg;
  vexo::gateway::UpstreamConfig upstream_cfg;
  upstream_cfg.name = "backend";
  upstream_cfg.max_concurrent_requests = max_concurrent;
  auto us = std::make_shared<vexo::gateway::Upstream>(std::move(upstream_cfg));
  for (uint16_t p : ports) {
    us->AddPeer(std::make_shared<vexo::gateway::UpstreamPeer>(vexo::gateway::UpstreamPeerConfig{
        .name = "127.0.0.1:" + std::to_string(p), .host = "127.0.0.1", .port = p}));
  }
  reg.Add(us);

  using Service =
      vexo::gateway::GatewaySessionService<vexo::net::ReactorStream, vexo::net::ReactorConnector>;
  using WorkerPool = Service::Pool;

  Service service("BenchGatewayMulti", reg);
  const vexo::gateway::PoolConfig pool_config{.max_idle_per_peer = max_idle_per_peer,
                                              .max_idle_total = max_idle_total};
  service.set_pool_config(pool_config);
  service.AddProxyRoute("/", "backend", algo);

  // Upstream streams are owned by their EventLoop. Keep one pool per worker
  // so sessions on the same loop can reuse connections without crossing
  // worker boundaries.
  std::vector<std::unique_ptr<WorkerPool>> worker_pools;
  std::unordered_map<vexo::net::EventLoop*, WorkerPool*> pools_by_loop;
  std::mutex pools_mutex;
  worker_pools.reserve(worker_num);
  pools_by_loop.reserve(worker_num);

  vexo::net::ReactorWorkerGroupOptions options;
  options.worker_num = worker_num;
  options.worker_options.listener_options.reuse_addr = true;
  options.worker_options.listener_options.reuse_port = true;

  vexo::net::ReactorWorkerGroup workers(
      vexo::net::InetAddress(listen_port), std::move(options),
      [&worker_pools, &pools_by_loop, &pools_mutex,
       pool_config](vexo::net::ReactorWorkerContext& context) {
        auto pool = std::make_unique<WorkerPool>(pool_config);
        auto* pool_ptr = pool.get();
        std::lock_guard lock(pools_mutex);
        worker_pools.push_back(std::move(pool));
        pools_by_loop.emplace(&context.loop, pool_ptr);
      },
      [&service, &pools_by_loop, &pools_mutex](
          vexo::net::ReactorWorkerContext& context,
          vexo::net::ReactorStream stream) -> vexo::coro::Task<void> {
        WorkerPool* pool = nullptr;
        {
          std::lock_guard lock(pools_mutex);
          auto it = pools_by_loop.find(&context.loop);
          if (it != pools_by_loop.end()) {
            pool = it->second;
          }
        }

        if (pool == nullptr) {
          co_await service.Serve(std::move(stream), vexo::net::ReactorConnector(&context.loop));
          co_return;
        }
        co_await service.Serve(std::move(stream), vexo::net::ReactorConnector(&context.loop),
                               *pool);
      });

  auto started = workers.Start();
  if (!started.has_value()) {
    std::fprintf(stderr, "ReactorWorkerGroup::Start failed: %s\n",
                 started.error().message().c_str());
    return 1;
  }

  std::printf(
      "BenchGatewayMulti listen=%u peers=[%s] algo=%s workers=%zu "
      "reuse_port=on max_concurrent=%zu max_idle_per_peer=%d max_idle_total=%zu\n",
      listen_port, ports_csv.c_str(), algo.c_str(), worker_num, max_concurrent,
      pool_config.max_idle_per_peer, pool_config.max_idle_total);
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return 0;
}
