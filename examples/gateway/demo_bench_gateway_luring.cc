// io_uring gateway benchmark with an optional coroutine-frame memory pool.
//
// The proxy topology intentionally matches demo_bench_gateway_multi:
// four local nginx upstreams, round-robin routing, and a configurable idle
// connection pool. Set FRAME_POOL=0 to retain the default new/delete frame
// allocator. For a fair comparison with nginx keepalive=64 per worker, use
// MAX_IDLE_PER_PEER=16 (4 peers x 16 idle connections per worker).
//
//   UPSTREAM_PORTS=9001,9002,9003,9004 \
//   BIND_HOST=0.0.0.0 FRAME_POOL=1 PORT=8081 \
//   ./build-uring/examples/gateway/demo_bench_gateway_luring

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <thread>
#include <vector>

#include "vexo/coro/frame_allocator.h"
#include "vexo/gateway/gateway_session_service.h"
#include "vexo/gateway/upstream.h"
#include "vexo/gateway/upstream_peer.h"
#include "vexo/gateway/upstream_registry.h"
#include "vexo/luring/connector.h"
#include "vexo/luring/server.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/net_utils.h"

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

const char* EnvOr(const char* key, const char* fallback) {
  const char* value = std::getenv(key);
  return value != nullptr ? value : fallback;
}

int EnvInt(const char* key, int fallback) {
  const char* value = std::getenv(key);
  return value != nullptr ? std::atoi(value) : fallback;
}

std::size_t EnvSize(const char* key, std::size_t fallback) {
  const char* value = std::getenv(key);
  if (value == nullptr) return fallback;
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(value, &end, 10);
  return end != value ? static_cast<std::size_t>(parsed) : fallback;
}

bool EnvBool(const char* key, bool fallback) {
  const char* value = std::getenv(key);
  if (value == nullptr) return fallback;
  return std::atoi(value) != 0;
}

}  // namespace

int main() {
  const uint16_t listen_port = static_cast<uint16_t>(EnvInt("PORT", 8081));
  const auto bind_host = std::string(EnvOr("BIND_HOST", "0.0.0.0"));
  const auto ports_csv = std::string(EnvOr("UPSTREAM_PORTS", "9001"));
  const auto algo = std::string(EnvOr("LB_ALGO", "round_robin"));
  const auto max_concurrent = EnvSize("MAX_CONCURRENT_REQUESTS", 1024);
  const auto max_idle_per_peer = EnvInt("MAX_IDLE_PER_PEER", 64);
  const auto ring_entries = static_cast<std::uint32_t>(EnvSize("URING_ENTRIES", 32768));
  const auto max_ready_work_per_turn = EnvSize("MAX_READY_WORK_PER_TURN", 256);
  const auto worker_num = EnvSize("URING_WORKERS", 4);
  const bool frame_pool = EnvBool("FRAME_POOL", true);

  if (worker_num == 0) {
    std::fprintf(stderr, "URING_WORKERS must be greater than zero\n");
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

  vexo::gateway::UpstreamRegistry registry;
  vexo::gateway::UpstreamConfig upstream_config;
  upstream_config.name = "backend";
  upstream_config.max_concurrent_requests = max_concurrent;
  auto upstream = std::make_shared<vexo::gateway::Upstream>(std::move(upstream_config));
  for (uint16_t port : ports) {
    upstream->AddPeer(
        std::make_shared<vexo::gateway::UpstreamPeer>(vexo::gateway::UpstreamPeerConfig{
            .name = "127.0.0.1:" + std::to_string(port), .host = "127.0.0.1", .port = port}));
  }
  registry.Add(std::move(upstream));

  using Service = vexo::gateway::GatewaySessionService<vexo::luring::LUringStream,
                                                       vexo::luring::LUringConnector>;
  using WorkerPool = Service::Pool;
  Service service("BenchGatewayLUring", registry);
  const vexo::gateway::PoolConfig pool_config{.max_idle_per_peer = max_idle_per_peer};
  service.set_pool_config(pool_config);
  service.AddProxyRoute("/", "backend", algo);

  // Upstream streams are owned by their io_uring loop. Keep one pool per
  // worker so sessions on the same ring can reuse connections without
  // introducing cross-worker stream ownership.
  std::vector<std::unique_ptr<WorkerPool>> worker_pools;
  std::unordered_map<vexo::luring::LUringLoop*, WorkerPool*> pools_by_loop;
  std::mutex pools_mutex;
  std::atomic_bool pools_ready{false};
  worker_pools.reserve(worker_num);
  pools_by_loop.reserve(worker_num);

  std::vector<std::unique_ptr<std::pmr::unsynchronized_pool_resource>> frame_pools;
  if (frame_pool) {
    frame_pools.reserve(worker_num);
    for (std::size_t i = 0; i < worker_num; ++i) {
      frame_pools.push_back(std::make_unique<std::pmr::unsynchronized_pool_resource>());
    }
  }

  vexo::luring::LUringServerOptions options;
  options.worker_group_options.worker_num = worker_num;
  options.worker_group_options.worker_options.loop_options.entries = ring_entries;
  options.worker_group_options.worker_options.loop_options.max_ready_work_per_turn =
      max_ready_work_per_turn;
  options.worker_group_options.worker_options.listen_options.reuse_port = true;
  options.worker_group_options.frame_resource_factory =
      [&frame_pools](std::size_t index) -> std::pmr::memory_resource* {
    return index < frame_pools.size() ? frame_pools[index].get() : nullptr;
  };

  auto listen_addr = vexo::net::ParseIPv4Address(bind_host, listen_port);
  if (!listen_addr.has_value()) {
    std::fprintf(stderr, "invalid BIND_HOST '%s': %s\n", bind_host.c_str(),
                 listen_addr.error().message().c_str());
    return 1;
  }

  vexo::luring::LUringServer server(*listen_addr, std::move(options));
  server.set_thread_init_callback(
      [&worker_pools, &pools_by_loop, &pools_mutex,
       pool_config](vexo::luring::LUringLoop* loop, vexo::luring::LUringListener*) {
        auto pool = std::make_unique<WorkerPool>(pool_config);
        auto* pool_ptr = pool.get();
        std::lock_guard lock(pools_mutex);
        worker_pools.push_back(std::move(pool));
        pools_by_loop.emplace(loop, pool_ptr);
      });
  server.set_session_handler(
      [&service, &pools_by_loop, &pools_mutex, &pools_ready](
          vexo::luring::LUringLoop& loop,
          std::unique_ptr<vexo::luring::LUringStream> stream) -> vexo::coro::Task<void> {
        WorkerPool* pool = nullptr;
        if (!pools_ready.load(std::memory_order_acquire)) {
          std::lock_guard lock(pools_mutex);
          auto it = pools_by_loop.find(&loop);
          if (it != pools_by_loop.end()) {
            pool = it->second;
          }
        } else {
          auto it = pools_by_loop.find(&loop);
          if (it != pools_by_loop.end()) {
            pool = it->second;
          }
        }
        if (pool == nullptr) {
          return service.Serve(std::move(stream), vexo::luring::LUringConnector(&loop));
        }
        return service.Serve(std::move(stream), vexo::luring::LUringConnector(&loop), *pool);
      });

  auto started = server.Start();
  if (!started.has_value()) {
    std::fprintf(stderr, "LUringServer::Start failed: %s\n", started.error().message().c_str());
    return 1;
  }
  pools_ready.store(true, std::memory_order_release);

  std::printf(
      "BenchGatewayLUring bind=%s listen=%u peers=[%s] algo=%s frame_pool=%s workers=%zu "
      "reuse_port=on entries=%u max_idle_per_peer=%d ready_budget=%zu\n",
      bind_host.c_str(), listen_port, ports_csv.c_str(), algo.c_str(), frame_pool ? "on" : "off",
      worker_num, ring_entries, max_idle_per_peer, max_ready_work_per_turn);
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  server.Stop();
  return 0;
}
