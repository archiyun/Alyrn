// io_uring gateway benchmark with an optional coroutine-frame memory pool.
//
// The proxy topology intentionally matches demo_bench_gateway_multi:
// four local nginx upstreams, round-robin routing, and 64 idle connections per
// peer. Set FRAME_POOL=0 to retain the default new/delete frame allocator.
//
//   UPSTREAM_PORTS=9001,9002,9003,9004 \
//   FRAME_POOL=1 PORT=8081 \
//   ./build-uring/examples/gateway/demo_bench_gateway_luring

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
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
  const auto ports_csv = std::string(EnvOr("UPSTREAM_PORTS", "9001"));
  const auto algo = std::string(EnvOr("LB_ALGO", "round_robin"));
  const auto max_concurrent = EnvSize("MAX_CONCURRENT_REQUESTS", 1024);
  const auto ring_entries = static_cast<std::uint32_t>(EnvSize("URING_ENTRIES", 32768));
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
  Service service("BenchGatewayLUring", registry);
  service.set_pool_config({.max_idle_per_peer = 64});
  service.AddProxyRoute("/", "backend", algo);

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
  options.worker_group_options.worker_options.listen_options.reuse_port = true;
  options.worker_group_options.frame_resource_factory =
      [&frame_pools](std::size_t index) -> std::pmr::memory_resource* {
    return index < frame_pools.size() ? frame_pools[index].get() : nullptr;
  };

  vexo::luring::LUringServer server(vexo::net::InetAddress(listen_port), std::move(options));
  server.set_session_handler(
      [&service](vexo::luring::LUringLoop& loop,
                 std::unique_ptr<vexo::luring::LUringStream> stream) -> vexo::coro::Task<void> {
        return service.Serve(std::move(stream), vexo::luring::LUringConnector(&loop));
      });

  auto started = server.Start();
  if (!started.has_value()) {
    std::fprintf(stderr, "LUringServer::Start failed: %s\n", started.error().message().c_str());
    return 1;
  }

  std::printf(
      "BenchGatewayLUring listen=%u peers=[%s] algo=%s frame_pool=%s workers=%zu "
      "reuse_port=on entries=%u\n",
      listen_port, ports_csv.c_str(), algo.c_str(), frame_pool ? "on" : "off", worker_num,
      ring_entries);
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  server.Stop();
  return 0;
}
