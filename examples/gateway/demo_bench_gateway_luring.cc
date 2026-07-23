// io_uring gateway benchmark with an optional size-class coroutine-frame pool.
//
// The proxy topology intentionally matches demo_bench_gateway_multi:
// four local nginx upstreams, round-robin routing, and a configurable idle
// connection pool. Set FRAME_POOL=0 to retain the default new/delete frame
// allocator. For a fair comparison with nginx keepalive=64 per worker, use
// MAX_IDLE_PER_PEER=0 MAX_IDLE_TOTAL=64 for one Worker-local shared budget.
//
//   UPSTREAM_PORTS=9001,9002,9003,9004 \
//   BIND_HOST=0.0.0.0 FRAME_POOL=1 PORT=8081 \
//   ./build-uring/examples/gateway/demo_bench_gateway_luring

#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "coropact/coro/frame_allocator.h"
#include "coropact/gateway/gateway_session_service.h"
#include "coropact/gateway/upstream.h"
#include "coropact/gateway/upstream_peer.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/luring/connector.h"
#include "coropact/luring/server.h"
#include "coropact/memory/pmr_pool_resource.h"
#include "coropact/net/inet_address.h"
#include "coropact/net/net_utils.h"

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

bool ParseCpuAffinity(std::string_view csv, std::vector<unsigned>& cpus) {
  cpus.clear();
  if (csv.empty()) return true;

  std::size_t begin = 0;
  while (begin <= csv.size()) {
    std::size_t end = csv.find(',', begin);
    if (end == std::string_view::npos) end = csv.size();
    const std::string_view token = csv.substr(begin, end - begin);
    if (token.empty()) return false;

    unsigned cpu = 0;
    const auto [parsed_end, error] =
        std::from_chars(token.data(), token.data() + token.size(), cpu);
    if (error != std::errc{} || parsed_end != token.data() + token.size()) return false;
    cpus.push_back(cpu);

    if (end == csv.size()) break;
    begin = end + 1;
  }
  return true;
}

}  // namespace

int main() {
  const uint16_t listen_port = static_cast<uint16_t>(EnvInt("PORT", 8081));
  const auto bind_host = std::string(EnvOr("BIND_HOST", "0.0.0.0"));
  const auto ports_csv = std::string(EnvOr("UPSTREAM_PORTS", "9001"));
  const auto algo = std::string(EnvOr("LB_ALGO", "round_robin"));
  const auto max_concurrent = EnvSize("MAX_CONCURRENT_REQUESTS", 1024);
  const auto max_idle_per_peer = EnvInt("MAX_IDLE_PER_PEER", 64);
  const auto max_idle_total = EnvSize("MAX_IDLE_TOTAL", 0);
  const auto ring_entries = static_cast<std::uint32_t>(EnvSize("URING_ENTRIES", 32768));
  const auto max_ready_work_per_turn = EnvSize("MAX_READY_WORK_PER_TURN", 256);
  const auto max_cqe_per_turn = EnvSize("MAX_CQE_PER_TURN", 256);
  const auto max_ready_time_us = EnvSize("MAX_READY_TIME_US", 50);
  const auto max_completion_work_per_turn = EnvSize("MAX_COMPLETION_WORK_PER_TURN", 64);
  const auto completion_age_threshold_us = EnvSize("COMPLETION_AGE_THRESHOLD_US", 0);
  const auto max_urgent_completion_work_per_turn =
      EnvSize("MAX_URGENT_COMPLETION_WORK_PER_TURN", 80);
  const auto normal_age_threshold_us = EnvSize("NORMAL_QUEUE_AGE_THRESHOLD_US", 5000);
  const auto worker_num = EnvSize("URING_WORKERS", 4);
  const auto cpu_affinity_csv = std::string(EnvOr("URING_CPU_AFFINITY", ""));
  const bool setup_sqpoll = EnvBool("URING_SQPOLL", false);
  const auto sqpoll_idle_ms = static_cast<std::uint32_t>(EnvSize("URING_SQPOLL_IDLE_MS", 1000));
  const bool setup_defer_taskrun = EnvBool("URING_DEFER_TASKRUN", false);
  const bool frame_pool = EnvBool("FRAME_POOL", true);
  const bool dump_stats = EnvBool("LURING_DUMP_STATS", false);
  const bool frame_stats_enabled = EnvBool("LURING_FRAME_STATS", false);

  if (worker_num == 0) {
    std::fprintf(stderr, "URING_WORKERS must be greater than zero\n");
    return 1;
  }

  std::vector<unsigned> cpu_affinity;
  if (!ParseCpuAffinity(cpu_affinity_csv, cpu_affinity) ||
      (!cpu_affinity.empty() && cpu_affinity.size() != worker_num)) {
    std::fprintf(stderr,
                 "URING_CPU_AFFINITY must be empty or contain exactly one CPU per worker "
                 "(comma-separated)\n");
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

  coropact::gateway::UpstreamRegistry registry;
  coropact::gateway::UpstreamConfig upstream_config;
  upstream_config.name = "backend";
  upstream_config.max_concurrent_requests = max_concurrent;
  auto upstream = std::make_shared<coropact::gateway::Upstream>(std::move(upstream_config));
  for (uint16_t port : ports) {
    upstream->AddPeer(
        std::make_shared<coropact::gateway::UpstreamPeer>(coropact::gateway::UpstreamPeerConfig{
            .name = "127.0.0.1:" + std::to_string(port), .host = "127.0.0.1", .port = port}));
  }
  registry.Add(std::move(upstream));

  using Service = coropact::gateway::GatewaySessionService<coropact::luring::LUringStream,
                                                       coropact::luring::LUringConnector>;
  using WorkerPool = Service::Pool;
  Service service("BenchGatewayLUring", registry);
  const coropact::gateway::PoolConfig pool_config{.max_idle_per_peer = max_idle_per_peer,
                                              .max_idle_total = max_idle_total,
                                              .collect_stats = dump_stats};
  service.set_pool_config(pool_config);
  service.AddProxyRoute("/", "backend", algo);

  // Upstream streams are owned by their io_uring loop. Keep one pool per
  // worker so sessions on the same ring can reuse connections without
  // introducing cross-worker stream ownership.
  std::vector<std::unique_ptr<WorkerPool>> worker_pools;
  std::unordered_map<coropact::luring::LUringLoop*, WorkerPool*> pools_by_loop;
  std::mutex pools_mutex;
  std::atomic_bool pools_ready{false};
  worker_pools.reserve(worker_num);
  pools_by_loop.reserve(worker_num);

  std::vector<coropact::memory::MemoryResourceStats> frame_resource_stats;
  std::vector<coropact::memory::MemoryResourceStats> frame_upstream_stats;
  std::vector<std::unique_ptr<coropact::memory::CountingMemoryResource>> frame_upstream_counters;
  std::vector<std::unique_ptr<coropact::coro::CoroFramePoolResource>> frame_pools;
  std::vector<std::unique_ptr<coropact::memory::CountingMemoryResource>> frame_counters;
  std::vector<std::unique_ptr<coropact::memory::CountingMemoryResource>> direct_frame_counters;
  if (frame_stats_enabled) {
    frame_resource_stats.resize(worker_num);
    frame_upstream_stats.resize(worker_num);
  }
  if (frame_pool) {
    if (frame_stats_enabled) {
      frame_upstream_counters.reserve(worker_num);
      frame_counters.reserve(worker_num);
    }
    frame_pools.reserve(worker_num);
    for (std::size_t i = 0; i < worker_num; ++i) {
      if (frame_stats_enabled) {
        frame_upstream_counters.push_back(std::make_unique<coropact::memory::CountingMemoryResource>(
            *std::pmr::new_delete_resource(), frame_upstream_stats[i]));
        frame_pools.push_back(
            std::make_unique<coropact::coro::CoroFramePoolResource>(*frame_upstream_counters.back()));
        frame_counters.push_back(std::make_unique<coropact::memory::CountingMemoryResource>(
            *frame_pools.back(), frame_resource_stats[i]));
      } else {
        frame_pools.push_back(std::make_unique<coropact::coro::CoroFramePoolResource>());
      }
    }
  } else if (frame_stats_enabled) {
    direct_frame_counters.reserve(worker_num);
    for (std::size_t i = 0; i < worker_num; ++i) {
      direct_frame_counters.push_back(std::make_unique<coropact::memory::CountingMemoryResource>(
          *std::pmr::new_delete_resource(), frame_resource_stats[i]));
    }
  }

  coropact::luring::LUringServerOptions options;
  options.worker_group_options.worker_num = worker_num;
  if (!cpu_affinity.empty()) {
    options.worker_group_options.cpu_affinity_factory =
        [cpu_affinity](std::size_t index) -> std::optional<unsigned> {
      return cpu_affinity[index];
    };
  }
  options.worker_group_options.worker_options.loop_options.entries = ring_entries;
  options.worker_group_options.worker_options.loop_options.setup_sqpoll = setup_sqpoll;
  options.worker_group_options.worker_options.loop_options.sqpoll_idle_ms = sqpoll_idle_ms;
  options.worker_group_options.worker_options.loop_options.setup_defer_taskrun =
      setup_defer_taskrun;
  options.worker_group_options.worker_options.loop_options.max_ready_work_per_turn =
      max_ready_work_per_turn;
  options.worker_group_options.worker_options.loop_options.max_cqe_per_turn = max_cqe_per_turn;
  options.worker_group_options.worker_options.loop_options.max_ready_time_per_turn =
      std::chrono::microseconds(max_ready_time_us);
  options.worker_group_options.worker_options.loop_options.max_completion_work_per_turn =
      max_completion_work_per_turn;
  options.worker_group_options.worker_options.loop_options.completion_queue_age_threshold =
      std::chrono::microseconds(completion_age_threshold_us);
  options.worker_group_options.worker_options.loop_options.max_urgent_completion_work_per_turn =
      max_urgent_completion_work_per_turn;
  options.worker_group_options.worker_options.loop_options.normal_queue_age_threshold =
      std::chrono::microseconds(normal_age_threshold_us);
  options.worker_group_options.worker_options.loop_options.dump_stats_on_exit = dump_stats;
  options.worker_group_options.worker_options.listen_options.reuse_port = true;
  options.worker_group_options.frame_resource_factory =
      [&frame_pools, &frame_counters, &direct_frame_counters, frame_pool,
       frame_stats_enabled](std::size_t index) -> std::pmr::memory_resource* {
    if (frame_pool) {
      if (index >= frame_pools.size()) return nullptr;
      if (frame_stats_enabled) {
        return frame_counters[index].get();
      }
      return frame_pools[index].get();
    }
    return frame_stats_enabled && index < direct_frame_counters.size()
               ? direct_frame_counters[index].get()
               : nullptr;
  };

  auto listen_addr = coropact::net::ParseIPv4Address(bind_host, listen_port);
  if (!listen_addr.has_value()) {
    std::fprintf(stderr, "invalid BIND_HOST '%s': %s\n", bind_host.c_str(),
                 listen_addr.error().message().c_str());
    return 1;
  }

  coropact::luring::LUringServer server(*listen_addr, std::move(options));
  server.set_thread_init_callback([&worker_pools, &pools_by_loop, &pools_mutex,
                                   pool_config](coropact::luring::LUringWorkerContext& context) {
    auto pool = std::make_unique<WorkerPool>(pool_config);
    auto* pool_ptr = pool.get();
    std::lock_guard lock(pools_mutex);
    worker_pools.push_back(std::move(pool));
    pools_by_loop.emplace(&context.loop, pool_ptr);
  });
  server.set_session_handler([&service, &pools_by_loop, &pools_mutex, &pools_ready](
                                 coropact::luring::LUringWorkerContext& context,
                                 coropact::luring::LUringStream stream) -> coropact::coro::Task<void> {
    auto& loop = context.loop;
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
      return service.Serve(std::move(stream), coropact::luring::LUringConnector(&loop));
    }
    return service.Serve(std::move(stream), coropact::luring::LUringConnector(&loop), *pool);
  });

  auto started = server.Start();
  if (!started.has_value()) {
    std::fprintf(stderr, "LUringServer::Start failed: %s\n", started.error().message().c_str());
    return 1;
  }
  pools_ready.store(true, std::memory_order_release);

  std::printf(
      "BenchGatewayLUring bind=%s listen=%u peers=[%s] algo=%s frame_pool=%s workers=%zu "
      "reuse_port=on entries=%u max_idle_per_peer=%d max_idle_total=%zu ready_budget=%zu "
      "cqe_budget=%zu "
      "ready_time_us=%zu completion_budget=%zu completion_age_us=%zu "
      "urgent_completion_budget=%zu normal_age_us=%zu sqpoll=%s sqpoll_idle_ms=%u "
      "defer_taskrun=%s "
      "cpu_affinity=%s dump_stats=%s frame_stats=%s\n",
      bind_host.c_str(), listen_port, ports_csv.c_str(), algo.c_str(), frame_pool ? "on" : "off",
      worker_num, ring_entries, max_idle_per_peer, max_idle_total, max_ready_work_per_turn,
      max_cqe_per_turn, max_ready_time_us, max_completion_work_per_turn,
      completion_age_threshold_us, max_urgent_completion_work_per_turn, normal_age_threshold_us,
      setup_sqpoll ? "on" : "off", sqpoll_idle_ms, setup_defer_taskrun ? "on" : "off",
      cpu_affinity.empty() ? "off" : cpu_affinity_csv.c_str(), dump_stats ? "on" : "off",
      frame_stats_enabled ? "on" : "off");
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  server.Stop();
  if (dump_stats) {
    const auto to_us = [](std::uint64_t nanoseconds) {
      return static_cast<double>(nanoseconds) / 1000.0;
    };
    const auto average_us = [](std::uint64_t total_ns, std::uint64_t samples) {
      return samples == 0 ? 0.0
                          : static_cast<double>(total_ns) / static_cast<double>(samples) / 1000.0;
    };
    const auto percentile_ms = [](const auto& histogram, std::uint64_t samples, double percentile) {
      if (samples == 0) return 0.0;
      std::uint64_t target = static_cast<std::uint64_t>(samples * percentile);
      if (target == 0) target = 1;
      std::uint64_t cumulative = 0;
      for (std::size_t bucket = 0; bucket < histogram.size(); ++bucket) {
        cumulative += histogram[bucket];
        if (cumulative >= target) {
          const std::uint64_t upper_ns = bucket + 1 < histogram.size()
                                             ? (std::uint64_t{1} << bucket)
                                             : std::numeric_limits<std::uint64_t>::max();
          return static_cast<double>(upper_ns) / 1'000'000.0;
        }
      }
      return static_cast<double>(std::numeric_limits<std::uint64_t>::max()) / 1'000'000.0;
    };
    for (std::size_t i = 0; i < worker_pools.size(); ++i) {
      const auto& stats = worker_pools[i]->stats();
      std::fprintf(
          stderr,
          "[gateway.pool] worker=%zu acquires=%llu hits=%llu misses=%llu releases=%llu "
          "release_drops=%llu idle=%zu connect_attempts=%llu connect_success=%llu "
          "connect_failures=%llu connect_avg_us=%.3f connect_max_us=%.3f "
          "connect_p95_ms=%.3f connect_p99_ms=%.3f "
          "upstream_write_avg_us=%.3f upstream_write_max_us=%.3f relay_avg_us=%.3f "
          "relay_max_us=%.3f relay_p95_ms=%.3f relay_p99_ms=%.3f "
          "upstream_write_p95_ms=%.3f upstream_write_p99_ms=%.3f\n",
          i, static_cast<unsigned long long>(stats.acquire_count),
          static_cast<unsigned long long>(stats.acquire_hit_count),
          static_cast<unsigned long long>(stats.acquire_miss_count),
          static_cast<unsigned long long>(stats.release_count),
          static_cast<unsigned long long>(stats.release_drop_count), worker_pools[i]->idle_count(),
          static_cast<unsigned long long>(stats.connect_attempt_count),
          static_cast<unsigned long long>(stats.connect_success_count),
          static_cast<unsigned long long>(stats.connect_failure_count),
          average_us(stats.connect_time_ns_sum, stats.connect_attempt_count),
          to_us(stats.connect_time_ns_max),
          percentile_ms(stats.connect_time_histogram, stats.connect_attempt_count, 0.95),
          percentile_ms(stats.connect_time_histogram, stats.connect_attempt_count, 0.99),
          average_us(stats.upstream_write_time_ns_sum, stats.upstream_write_count),
          to_us(stats.upstream_write_time_ns_max),
          average_us(stats.relay_time_ns_sum, stats.relay_count), to_us(stats.relay_time_ns_max),
          percentile_ms(stats.relay_time_histogram, stats.relay_count, 0.95),
          percentile_ms(stats.relay_time_histogram, stats.relay_count, 0.99),
          percentile_ms(stats.upstream_write_time_histogram, stats.upstream_write_count, 0.95),
          percentile_ms(stats.upstream_write_time_histogram, stats.upstream_write_count, 0.99));
    }
  }
  if (frame_stats_enabled) {
    for (std::size_t i = 0; i < worker_num; ++i) {
      const auto& frame = frame_resource_stats[i];
      const auto& upstream = frame_upstream_stats[i];
      std::fprintf(
          stderr,
          "[coro.frame] worker=%zu alloc_calls=%llu dealloc_calls=%llu allocated_bytes=%llu "
          "deallocated_bytes=%llu live_allocations=%llu live_bytes=%llu "
          "peak_live_allocations=%llu peak_live_bytes=%llu upstream_alloc_calls=%llu "
          "upstream_allocated_bytes=%llu upstream_live_bytes=%llu\n",
          i, static_cast<unsigned long long>(frame.allocate_calls),
          static_cast<unsigned long long>(frame.deallocate_calls),
          static_cast<unsigned long long>(frame.allocated_bytes),
          static_cast<unsigned long long>(frame.deallocated_bytes),
          static_cast<unsigned long long>(frame.outstanding_allocations),
          static_cast<unsigned long long>(frame.outstanding_bytes),
          static_cast<unsigned long long>(frame.peak_outstanding_allocations),
          static_cast<unsigned long long>(frame.peak_outstanding_bytes),
          static_cast<unsigned long long>(upstream.allocate_calls),
          static_cast<unsigned long long>(upstream.allocated_bytes),
          static_cast<unsigned long long>(upstream.outstanding_bytes));
    }
  }
  return 0;
}
