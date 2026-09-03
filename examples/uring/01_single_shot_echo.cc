// Build:
//   make uring
// Run:
//   ./build/uring/debug/examples/uring/demo_luring_single_shot_echo

#include <array>
#include <csignal>
#include <cstddef>
#include <print>
#include <span>
#include <stop_token>
#include <utility>

#include "alyrn/coro/spawn.h"
#include "alyrn/io.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/uring.h"
#include "alyrn/uring/listener.h"
#include "alyrn/uring/stream.h"

using namespace alyrn;

namespace {

constexpr std::uint16_t kPort = 19090;

auto EchoSession(uring::Stream stream) -> alyrn::DetachedTask {
  std::array<std::byte, 4096> buffer{};

  for (;;) {
    auto read = co_await stream.Read(buffer);

    if (!read.HasValue()) {
      std::println(stderr, "read failed: {}", read.Error().message());
      break;
    }

    if (*read == 0) {
      break;
    }

    auto payload = std::span<const std::byte>(buffer.data(), *read);

    auto written = co_await stream.Write(payload);
    if (!written.HasValue()) {
      std::println(stderr, "write failed: {}", written.Error().message());
      break;
    }
  }

  auto closed = co_await stream.Close();
  if (!closed.HasValue()) {
    std::println(stderr, "close failed: {}", closed.Error().message());
  }
}

auto AcceptLoop(uring::Loop& loop, uring::Listener& listener) -> alyrn::DetachedTask {
  for (;;) {
    auto accepted = co_await listener.Accept();

    if (!accepted.HasValue()) {
      std::println(stderr, "accept failed: {}", accepted.Error().message());
      co_return;
    }

    alyrn::SpawnDetach(loop, EchoSession(std::move(*accepted)));
  }
}

}  // namespace

auto main() -> int {
  std::signal(SIGPIPE, SIG_IGN);

  uring::Loop loop;

  uring::Options options;
  options.entries = 256;

  auto initialized = loop.Init(options);

  if (!initialized.HasValue()) {
    std::println(stderr, "loop init failed: {}", initialized.Error().message());
    return 1;
  }

  auto listener_result = uring::Listener::Create(&loop, net::Endpoint::Loopback(kPort));

  if (!listener_result.HasValue()) {
    std::println(stderr, "listener create failed: {}", listener_result.Error().message());
    return 1;
  }

  auto listener = std::move(*listener_result);

  alyrn::SpawnDetach(loop, AcceptLoop(loop, listener));

  std::println("single-shot echo listening on 127.0.0.1:{}", kPort);

  loop.Run(std::stop_token{});

  return 0;
}
