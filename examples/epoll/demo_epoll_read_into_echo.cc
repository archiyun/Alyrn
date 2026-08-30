// SPDX-License-Identifier: MIT
//
// Epoll ReadInto echo demo
//
// Build:
//   cmake --build build --target demo_epoll_read_into_echo -j"$(nproc)"
//
// Run:
//   ./build/examples/epoll/demo_epoll_read_into_echo
//
// Try:
//   nc 127.0.0.1 19001

#include <csignal>
#include <cstddef>
#include <cstdint>
#include <print>
#include <stop_token>
#include <utility>

#include "alyrn/coro.h"
#include "alyrn/io.h"
#include "alyrn/io/buffer.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/epoll.h"
#include "alyrn/epoll/listener.h"
#include "alyrn/epoll/loop.h"

using namespace alyrn;

namespace {

constexpr std::uint16_t kPort = 19091;
constexpr std::size_t kReadReserve = 4096;

auto EchoSession(epoll::Stream stream) -> alyrn::DetachedTask {
  io::Buffer buffer{kReadReserve};

  for (;;) {
    // ReadInto moves the buffer into the pending operation. Do not access
    // 'buffer' until await_resume returns its ReadIntoOutcome.
    auto [read, returned_buffer] = co_await stream.ReadInto(std::move(buffer), kReadReserve);

    // Every terminal path returns ownership of the buffer: successful read,
    // EOF, or I/O error, cancellation, and shutdown.
    buffer = std::move(returned_buffer);

    if (!read.has_value()) {
      std::println(stderr, "read failed: {}", read.error().message());
      break;
    }

    if (*read == 0) {
      break;
    }

    // The completed read has already committed bytes into 'buffer'. Each
    // WriteAll call owns one contiguous borrowed view; drain it after success.
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

auto AcceptLoop(epoll::Loop& loop, epoll::Listener& listener)
    -> alyrn::DetachedTask {
  for (;;) {
    auto accepted = co_await listener.Accept();
    if (!accepted.has_value()) {
      std::println(stderr, "accept failed: {}", accepted.error().message());
      co_return;
    }

    alyrn::SpawnDetach(loop, EchoSession(std::move(*accepted)));
  }
}

}  // namespace

auto main() -> int {
  std::signal(SIGPIPE, SIG_IGN);

  epoll::Loop loop;

  auto listener_result = epoll::Listener::Create(&loop, net::Endpoint::Loopback(kPort));
  if (!listener_result.has_value()) {
    std::println(stderr, "listener create failed: {}", listener_result.error().message());
    return 1;
  }

  auto listener = std::move(*listener_result);
  alyrn::SpawnDetach(loop, AcceptLoop(loop, listener));

  std::println("Epoll ReadInto echo listening on 127.0.0.1:{}", kPort);
  loop.Run(std::stop_token{});
  return 0;
}
