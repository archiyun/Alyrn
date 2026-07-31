// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <span>
#include <thread>
#include <vector>

#include "bench_http_common.h"
#include "coropact/coro/frame_allocator.h"
#include "coropact/io.h"
#include "coropact/net/endpoint.h"
#include "coropact/reactor/reactor_worker_group.h"

namespace {

std::atomic_bool g_stop{false};
void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

coropact::coro::DetachedTask HttpSession(coropact::reactor::ReactorStream stream) {
  std::array<std::byte, coropact_bench::kRequestBufferSize> request{};
  const auto response = std::as_bytes(
      std::span(coropact_bench::Response().data(), coropact_bench::Response().size()));
  std::size_t used = 0;

  for (;;) {
    auto read = co_await stream.ReadSome(std::span(request).subspan(used));
    if (!read.has_value() || *read == 0) break;
    used += *read;
    if (!coropact_bench::HasHeaderTerminator(reinterpret_cast<const char*>(request.data()), used)) {
      if (used == request.size()) break;
      continue;
    }

    auto written = co_await coropact::io::WriteAll(stream, response);
    if (!written.has_value()) break;
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
  const std::size_t workers = coropact_bench::EnvSize("REACTOR_WORKERS", 4);
  if (port == 0 || workers == 0) return 2;

  auto address = coropact::net::Endpoint::Loopback(port);
  coropact::reactor::ReactorWorkerGroupOptions options;
  options.worker_num = workers;
  options.worker_options.listener_options.reuse_addr = true;
  options.worker_options.listener_options.reuse_port = true;

  coropact::reactor::ReactorWorkerGroup server(
      address, std::move(options), {},
      [](coropact::reactor::ReactorWorkerContext&, coropact::reactor::ReactorStream stream) {
        return HttpSession(std::move(stream));
      });
  auto started = server.Start();
  if (!started.has_value()) {
    std::cerr << "ReactorWorkerGroup::Start failed: " << started.error().message() << '\n';
    return 1;
  }

  std::cout << "HttpReactorBench bind=127.0.0.1 port=" << port << " workers=" << workers
            << " response_body=" << coropact_bench::kResponseBodySize << '\n';
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  server.Stop();
  return 0;
}
