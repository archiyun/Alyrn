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

#include <csignal>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <print>
#include <stop_token>
#include <utility>

#include "coropact/io.h"
#include "coropact/luring.h"
#include "coropact/net.h"
#include "echo_app.h"

using namespace coropact;

namespace {

constexpr std::uint16_t kPort = 9090;
constexpr std::uint32_t kEntries = 4096;

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);

  luring::LUringOptions loop_options;
  loop_options.entries = kEntries;

  luring::LUringLoop loop;
  auto initialized = loop.Init(loop_options);
  if (!initialized.has_value()) {
    std::println(stderr, "failed to initialize Luring loop: {}",
                 initialized.error().message());
    return 1;
  }

  auto listener_result =
      luring::LUringListener::Create(&loop, net::Endpoint::Loopback(kPort));
  if (!listener_result.has_value()) {
    std::println(stderr, "failed to create Luring listener: {}",
                 listener_result.error().message());
    return 1;
  }

  auto listener = std::move(*listener_result);
  coro::SpawnDetach(loop, simple_echo::AcceptLoop(listener, loop));

  std::println(std::cout, "simple echo (Luring) listening on 127.0.0.1:{}", kPort);
  loop.Run(std::stop_token{});
  return 0;
}
