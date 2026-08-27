#include <array>
#include <csignal>
#include <cstddef>
#include <print>
#include <span>
#include <stop_token>
#include <utility>

#include "alyrn/coro/detached_task.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/io.h"
#include "alyrn/luring.h"
#include "alyrn/luring/listener.h"
#include "alyrn/luring/stream.h"
#include "alyrn/net/endpoint.h"

using namespace alyrn;

namespace {

constexpr std::uint16_t kPort = 19090;

auto EchoSession(luring::Stream stream) -> coro::DetachedTask {
  std::array<std::byte, 4096> buffer{};

  for (;;) {
    auto read = co_await stream.ReadSome(buffer);

    if (!read.has_value()) {
      std::println(stderr, "read failed: {}", read.error().message());
      break;
    }

    if (*read == 0) {
      break;
    }

    auto payload = std::span<const std::byte>(buffer.data(), *read);

    auto written = co_await stream.WriteAll(payload);
    if (!written.has_value()) {
      std::println(stderr, "write failed: {}", written.error().message());
      break;
    }
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

  std::println("single-shot echo listening on 127.0.0.1:{}", kPort);

  loop.Run(std::stop_token{});

  return 0;
}
