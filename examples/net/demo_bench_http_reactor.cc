// SPDX-License-Identifier: MIT

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <iostream>
#include <span>
#include <thread>

#include "bench_http_common.h"
#include "coropact/io.h"
#include "coropact/net/endpoint.h"
#include "coropact/reactor/detail/worker_group.h"

namespace {

std::atomic_bool g_stop{false};
void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

coropact::coro::DetachedTask HttpSession(coropact::reactor::Stream stream) {
  std::array<std::byte, coropact_bench::kRequestBufferSize> request{};
  const auto response = std::as_bytes(
      std::span(coropact_bench::Response().data(), coropact_bench::Response().size()));
  std::size_t used = 0;

  for (;;) {
    const std::size_t previous_used = used;
    auto read = co_await stream.ReadSome(std::span(request).subspan(used));
    if (!read.has_value() || *read == 0) break;
    used += *read;
    const std::size_t scan_from = previous_used >= 3 ? previous_used - 3 : 0;
    if (!coropact_bench::HasHeaderTerminator(reinterpret_cast<const char*>(request.data()), used,
                                             scan_from)) {
      if (used == request.size()) break;
      continue;
    }

    auto written = co_await stream.WriteAll(response);
    if (!written.has_value()) break;
    used = 0;
  }
  (void)co_await stream.Close();
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  const auto port = static_cast<std::uint16_t>(coropact_bench::EnvInt("PORT", 19090));
  const std::size_t workers = coropact_bench::EnvSize("REACTOR_WORKERS", 8);
  const bool level_triggered = coropact_bench::EnvString("REACTOR_TRIGGER_MODE") == "lt";
  const auto trigger_mode = level_triggered ? coropact::reactor::TriggerMode::kLevelTriggered
                                            : coropact::reactor::TriggerMode::kEdgeTriggered;
  if (port == 0 || workers == 0) return 2;

  auto address = coropact::net::Endpoint::Loopback(port);
  coropact::reactor::detail::WorkerGroupOptions options;
  options.worker_num = workers;
  options.worker_options.listener_options.reuse_addr = true;
  options.worker_options.listener_options.reuse_port = true;
  options.worker_options.listener_options.stream_options.trigger_mode = trigger_mode;
  options.worker_options.connector_options.stream_options.trigger_mode = trigger_mode;

  coropact::reactor::detail::WorkerGroup server(
      address, std::move(options), {},
      [](coropact::reactor::detail::WorkerContext&,
         coropact::reactor::Stream stream) { return HttpSession(std::move(stream)); });
  auto started = server.Start();
  if (!started.has_value()) {
    std::cerr << "WorkerGroup::Start failed: " << started.error().message() << '\n';
    return 1;
  }

  std::cout << "HttpReactorBench bind=127.0.0.1 port=" << port << " workers=" << workers
            << " trigger_mode=" << (level_triggered ? "lt" : "et")
            << " response_body=" << coropact_bench::kResponseBodySize << '\n';
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  server.Stop();
  return 0;
}
