// SPDX-License-Identifier: MIT
//
// LUring ReadInto echo demo
//
// Build:
//   cmake --build build-uring --target demo_luring_read_into_echo -j"$(nproc)"
//
// Run:
//   ./build-uring/examples/demo_luring_read_into_echo
//
// Try:
//   nc 127.0.0.1 19092

#include <csignal>
#include <cstddef>
#include <cstdint>
#include <print>
#include <stop_token>
#include <utility>

#include "alyrn/coro.h"
#include "alyrn/io.h"
#include "alyrn/luring.h"
#include "alyrn/net.h"

using namespace alyrn;

namespace {

constexpr std::uint16_t kPort = 19092;
constexpr std::size_t kReadReserve = 4096;

auto EchoSession(luring::Stream stream) -> coro::DetachedTask {
  io::Buffer buffer{kReadReserve};

  for (;;) {
    auto [read, returned_buffer] = co_await stream.ReadInto(std::move(buffer), kReadReserve);

    buffer = std::move(returned_buffer);

    if (!read.has_value()) {
      std::println(stderr, "read failed: {}", read.error().message());
      break;
    }

    if (*read == 0) {
      break;
    }

    while (!buffer.Empty()) {
      auto view = buffer.ContiguousView();
      auto written = co_await stream.WriteAll(view);
      if (!written.has_value()) {
        std::println(stderr, "write failed: {}", written.error().message());
        break;
      }
      buffer.Drain(view.size());
    }

    if (!buffer.Empty()) break;
  }

  auto closed = co_await stream.Close();
  if (!closed.has_value()) {
    std::println(stderr, "close failed: {}", closed.error().message());
  }
}

auto AcceptLoop(luring::Loop& loop, luring::Listener& listener) -> coro::DetachedTask {
  for (;;) {
    auto accepted = co_await listener.Accept();
    if (!accepted.has_value()) {
      std::println(stderr, "accept failed: {}", accepted.error().message());
      co_return;
    }

    coro::SpawnDetach(loop, EchoSession(std::move(*accepted)));
  }
}

}  // namespace

auto main() -> int {
  std::signal(SIGPIPE, SIG_IGN);

  luring::Loop loop;

  luring::Options options;
  options.entries = 256;

  auto initialized = loop.Init(options);
  if (!initialized.has_value()) {
    std::println(stderr, "loop init failed: {}", initialized.error().message());
    return 1;
  }

  auto listener_result = luring::Listener::Create(&loop, net::Endpoint::Loopback(kPort));
  if (!listener_result.has_value()) {
    std::println(stderr, "listener create failed: {}", listener_result.error().message());
    return 1;
  }

  auto listener = std::move(*listener_result);
  coro::SpawnDetach(loop, AcceptLoop(loop, listener));

  std::println("LUring ReadInto echo listening on 127.0.0.1:{}", kPort);

  loop.Run(std::stop_token{});
  return 0;
}
