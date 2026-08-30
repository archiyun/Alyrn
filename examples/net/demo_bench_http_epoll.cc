// SPDX-License-Identifier: MIT

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <thread>

#include "bench_http_common.h"
#include "alyrn/io.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/epoll/detail/worker_group.h"
#include "alyrn/spawn.h"

namespace {

std::atomic_bool g_stop{false};
// Set EPOLL_INSTRUMENT=1 to collect operation counters. Keep it disabled
// for throughput runs because the global counters add cross-worker traffic.
bool g_instrument = false;
std::atomic<std::uint64_t> g_sessions{0};
std::atomic<std::uint64_t> g_read_awaits{0};
std::atomic<std::uint64_t> g_reads_completed{0};
std::atomic<std::uint64_t> g_write_awaits{0};
std::atomic<std::uint64_t> g_responses{0};
void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

inline void Count(std::atomic<std::uint64_t>& counter) noexcept {
  if (g_instrument) {
    counter.fetch_add(1, std::memory_order_relaxed);
  }
}

alyrn::DetachedTask HttpSession(alyrn::epoll::Stream stream) {
  Count(g_sessions);
  std::array<std::byte, alyrn_bench::kRequestBufferSize> request{};
  const auto response = std::as_bytes(
      std::span(alyrn_bench::Response().data(), alyrn_bench::Response().size()));
  std::size_t used = 0;

  for (;;) {
    const std::size_t previous_used = used;
    Count(g_read_awaits);
    auto read = co_await stream.ReadSome(std::span(request).subspan(used));
    if (!read.has_value() || *read == 0) break;
    Count(g_reads_completed);
    used += *read;
    const std::size_t scan_from = previous_used >= 3 ? previous_used - 3 : 0;
    if (!alyrn_bench::HasHeaderTerminator(reinterpret_cast<const char*>(request.data()), used,
                                             scan_from)) {
      if (used == request.size()) break;
      continue;
    }

    Count(g_write_awaits);
    auto written = co_await stream.WriteAll(response);
    if (!written.has_value()) break;
    Count(g_responses);
    used = 0;
  }
  (void)co_await stream.Close();
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  const auto port = static_cast<std::uint16_t>(alyrn_bench::EnvInt("PORT", 19090));
  const std::size_t workers = alyrn_bench::EnvSize("EPOLL_WORKERS", 8);
  const bool level_triggered = alyrn_bench::EnvString("EPOLL_TRIGGER_MODE") == "lt";
  const auto trigger_mode = level_triggered ? alyrn::epoll::TriggerMode::kLevelTriggered
                                            : alyrn::epoll::TriggerMode::kEdgeTriggered;
  g_instrument = alyrn_bench::EnvInt("EPOLL_INSTRUMENT", 0) != 0;
  if (port == 0 || workers == 0) return 2;

  auto address = alyrn::net::Endpoint::Loopback(port);
  alyrn::epoll::detail::WorkerGroupOptions options;
  options.worker_num = workers;
  options.worker_options.listener_options.reuse_addr = true;
  options.worker_options.listener_options.reuse_port = true;
  options.worker_options.listener_options.stream_options.trigger_mode = trigger_mode;
  options.worker_options.connector_options.stream_options.trigger_mode = trigger_mode;

  alyrn::epoll::detail::WorkerGroup server(
      address, std::move(options), {},
      [](alyrn::epoll::detail::WorkerContext&,
         alyrn::epoll::Stream stream) { return HttpSession(std::move(stream)); });
  auto started = server.Start();
  if (!started.has_value()) {
    std::cerr << "WorkerGroup::Start failed: " << started.error().message() << '\n';
    return 1;
  }

  std::cout << "HttpEpollBench bind=127.0.0.1 port=" << port << " workers=" << workers
            << " trigger_mode=" << (level_triggered ? "lt" : "et")
            << " response_body=" << alyrn_bench::kResponseBodySize << '\n';
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  server.Stop();
  if (g_instrument) {
    const auto sessions = g_sessions.load(std::memory_order_relaxed);
    const auto read_awaits = g_read_awaits.load(std::memory_order_relaxed);
    const auto reads_completed = g_reads_completed.load(std::memory_order_relaxed);
    const auto write_awaits = g_write_awaits.load(std::memory_order_relaxed);
    const auto responses = g_responses.load(std::memory_order_relaxed);
    std::cout << "instrumentation sessions=" << sessions << " read_awaits=" << read_awaits
              << " reads_completed=" << reads_completed << " write_awaits=" << write_awaits
              << " responses=" << responses;
    if (responses != 0) {
      std::cout << " read_awaits_per_response="
                << static_cast<double>(read_awaits) / static_cast<double>(responses)
                << " reads_per_response="
                << static_cast<double>(reads_completed) / static_cast<double>(responses);
      std::cout << " write_awaits_per_response="
                << static_cast<double>(write_awaits) / static_cast<double>(responses);
    }
    std::cout << '\n';
  }
  return 0;
}
