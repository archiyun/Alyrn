// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Build:
//   cmake --build build --target simple_echo -j
//
// Run:
//   ./build/examples/simple_echo
//
// Try:
//   nc 127.0.0.1 9090

#include <csignal>
#include <cstdint>
#include <iostream>
#include <utility>

#include "coropact/coro.h"
#include "coropact/io.h"
#include "coropact/net.h"
#include "coropact/reactor.h"
#include "echo_app.h"

using namespace coropact;

namespace {

constexpr std::uint16_t kPort = 9090;

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);

  reactor::EventLoop loop;
  const auto address = net::Endpoint::Loopback(kPort);

  auto listener_result = reactor::ReactorListener::Create(&loop, address);
  if (!listener_result.has_value()) {
    std::println(stderr, "failed to create Reactor listener: {}",
                 listener_result.error().message());
    return 1;
  }

  auto listener = std::move(*listener_result);
  coro::SpawnDetach(loop, simple_echo::AcceptLoop(listener, loop));

  std::println(std::cout, "simple echo (Reactor) listening on 127.0.0.1:{}", kPort);
  loop.Loop();
  return 0;
}
