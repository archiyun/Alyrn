// SPDX-License-Identifier: MIT

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <span>
#include <thread>
#include <utility>

#include "bench_http_common.h"
#include "coropact/io.h"
#include "coropact/luring/detail/worker_group.h"
#include "coropact/net/endpoint.h"

namespace {

std::atomic_bool g_stop{false};

void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

coropact::coro::DetachedTask HttpSession(coropact::luring::LUringStream stream) {
  std::array<std::byte, coropact_bench::kRequestBufferSize> request{};
  const auto response = std::as_bytes(
      std::span(coropact_bench::Response().data(), coropact_bench::Response().size()));
  std::size_t used = 0;

  for (;;) {
    auto read = co_await stream.ReadSome(std::span(request).subspan(used));
    if (!read.has_value() || *read == 0) {
      break;
    }
    used += *read;
    if (!coropact_bench::HasHeaderTerminator(reinterpret_cast<const char*>(request.data()), used)) {
      if (used == request.size()) {
        break;
      }
      continue;
    }

    auto written = co_await stream.WriteAll(response);
    if (!written.has_value()) {
      break;
    }
    used = 0;
  }
  co_await stream.Close();
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  const auto port = static_cast<std::uint16_t>(coropact_bench::EnvInt("PORT", 19090));
  const std::size_t workers = coropact_bench::EnvSize("URING_WORKERS", 4);
  const auto entries = static_cast<std::uint32_t>(coropact_bench::EnvSize("URING_ENTRIES", 1024));
  if (port == 0 || workers == 0) {
    return 2;
  }

  coropact::luring::detail::LUringWorkerGroupOptions options;
  options.worker_num = workers;
  options.worker_options.loop_options.entries = entries;
  options.worker_options.listen_options.reuse_addr = true;
  options.worker_options.listen_options.reuse_port = true;
  options.worker_options.accept_mode = coropact::luring::detail::AcceptMode::kSingleShot;

  coropact::luring::detail::LUringWorkerGroup server(
      coropact::net::Endpoint::Loopback(port), std::move(options), {},
      [](coropact::luring::detail::LUringWorkerContext&, coropact::luring::LUringStream stream) {
        return HttpSession(std::move(stream));
      });
  auto started = server.Start();
  if (!started.has_value()) {
    std::fprintf(stderr, "LUringWorkerGroup::Start failed: %s\n",
                 started.error().message().c_str());
    return 1;
  }

  std::printf(
      "HttpLuringBench bind=127.0.0.1 port=%u workers=%zu entries=%u accept=single-shot "
      "frame_pool=off response_body=%zu\n",
      port, workers, entries, coropact_bench::kResponseBodySize);
  std::fflush(stdout);
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  server.Stop();
  return 0;
}
