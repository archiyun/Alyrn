#include <array>
#include <csignal>
#include <cstddef>
#include <print>
#include <span>
#include <stop_token>
#include <utility>

#include "coropact/coro/detached_task.h"
#include "coropact/coro/spawn.h"
#include "coropact/io.h"
#include "coropact/luring.h"
#include "coropact/luring/listener.h"
#include "coropact/luring/stream.h"
#include "coropact/net/endpoint.h"

using namespace coropact;

namespace {

constexpr std::uint16_t kPort = 19090;

coro::DetachedTask EchoSession(luring::LUringStream stream) {
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

    auto written = co_await io::WriteAll(stream, payload);
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

coro::DetachedTask AcceptLoop(luring::LUringLoop& loop, luring::LUringListener& listener) {
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

int main() {
  std::signal(SIGPIPE, SIG_IGN);

  luring::LUringLoop loop;

  luring::LUringOptions options;
  options.entries = 256;
  options.submit_batch = 1;

  auto initialized = loop.Init(options);

  if (!initialized.has_value()) {
    std::println(stderr, "loop init failed: {}", initialized.error().message());
    return 1;
  }

  auto listener_result = luring::LUringListener::Create(&loop, net::Endpoint::Loopback(kPort));

  if (!listener_result.has_value()) {
    std::println(stderr, "listener create failed: {}", listener_result.error().message());
    return 1;
  }

  auto listener = std::move(*listener_result);

  coro::SpawnDetach(loop, AcceptLoop(loop, listener));

  std::println("single-shot echo listening on 127.0.0.1:{}", kPort);

  loop.Loop(std::stop_token{});
}
