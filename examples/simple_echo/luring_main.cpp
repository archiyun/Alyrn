// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Build:
//   cmake -S . -B build-uring -DBUILD_EXAMPLES=ON -DCOROPACT_ENABLE_URING=ON
//   cmake --build build-uring --target simple_echo_luring -j
//
// Run:
//   ./build-uring/examples/simple_echo_luring
//
// Try:
//   nc 127.0.0.1 9090

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <thread>
#include <utility>

#include "coropact/io.h"
#include "coropact/luring.h"
#include "coropact/net.h"
#include "echo_app.h"

using namespace coropact;

namespace {

constexpr std::uint16_t kPort = 9090;
constexpr std::size_t kWorkers = 1;
constexpr std::uint32_t kEntries = 4096;

std::atomic_bool g_stop{false};

void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  luring::LUringOptions loop_options;
  loop_options.entries = kEntries;

  auto binding = luring::BindLUring(loop_options, luring::RuntimeProfile::Core());
  if (!binding.has_value()) {
    std::cerr << "failed to bind Luring backend: " << binding.error().message() << '\n';
    return 1;
  }

  luring::LUringServerOptions server_options;
  server_options.worker_group_options.worker_num = kWorkers;
  server_options.worker_group_options.worker_options.loop_options = loop_options;
  server_options.worker_group_options.worker_options.listen_options.accept_depth = 1;

  luring::LUringServer server(net::Endpoint::Loopback(kPort), std::move(server_options));
  server.SetSessionHandler([](luring::LUringWorkerContext&, luring::LUringStream stream) {
    return simple_echo::EchoSession(std::move(stream));
  });

  auto started = server.Start();
  if (!started.has_value()) {
    std::cerr << "failed to start Luring server: " << started.error().message() << '\n';
    return 1;
  }

  std::cout << "simple echo (Luring) listening on 127.0.0.1:" << kPort << '\n';
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  server.Stop();
  return 0;
}
