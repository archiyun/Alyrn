#include <array>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <print>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>

#include "coropact/coro/detached_task.h"
#include "coropact/coro/spawn.h"
#include "coropact/io.h"
#include "coropact/luring/connector.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/stream.h"

using namespace coropact;

namespace {

constexpr std::uint16_t kEchoPort = 19090;
constexpr std::string_view kMessage = "hello from connector";

// Connects to the server from 01_single_shot_echo.cc, writes one message,
// reads the complete echo, and then closes the stream.
//
// exit_code is a reference to main's state. The coroutine always runs on the
// same Loop thread, so it is safe to update it before RequestStop().
coro::DetachedTask ConnectOnce(luring::Loop& loop, int& exit_code) {
  luring::Connector connector(&loop);

  auto connected = co_await connector.Connect("127.0.0.1", kEchoPort);
  if (!connected.has_value()) {
    std::println(stderr, "connect failed: {}", connected.error().message());
    loop.RequestStop();
    co_return;
  }

  auto stream = std::move(*connected);
  std::println("connected to 127.0.0.1:{}", kEchoPort);

  const auto payload = std::as_bytes(std::span<const char>(kMessage.data(), kMessage.size()));

  auto write_result = co_await stream.WriteAll(payload);
  if (!write_result.has_value()) {
    std::println(stderr, "write failed: {}", write_result.error().message());
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  std::println("sent {} bytes", payload.size());

  std::array<std::byte, 1024> buffer{};
  std::string received;
  received.reserve(kMessage.size());

  // ReadSome may return a partial read, so keep reading until the complete
  // message has arrived instead of assuming one CQE equals one message.
  while (received.size() < kMessage.size()) {
    auto read = co_await stream.ReadSome(buffer);

    if (!read.has_value()) {
      std::println(stderr, "read failed: {}", read.error().message());
      (void)co_await stream.Close();
      loop.RequestStop();
      co_return;
    }

    if (*read == 0) {
      std::println(stderr, "peer closed before the complete echo arrived");
      (void)co_await stream.Close();
      loop.RequestStop();
      co_return;
    }

    received.append(reinterpret_cast<const char*>(buffer.data()), *read);

    if (received.size() > kMessage.size()) {
      std::println(stderr, "received more bytes than expected");
      (void)co_await stream.Close();
      loop.RequestStop();
      co_return;
    }
  }

  if (received != kMessage) {
    std::println(stderr, "echo mismatch");
    std::println(stderr, "expected: {}", kMessage);
    std::println(stderr, "received: {}", received);
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  std::println("received: {}", received);
  std::println("echo check passed");

  auto closed = co_await stream.Close();
  if (!closed.has_value()) {
    std::println(stderr, "close failed: {}", closed.error().message());
    loop.RequestStop();
    co_return;
  }

  std::println("stream closed");
  exit_code = 0;
  loop.RequestStop();
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);

  luring::Loop loop;

  luring::Options options;
  options.entries = 64;

  auto initialized = loop.Init(options);
  if (!initialized.has_value()) {
    std::println(stderr, "loop init failed: {}", initialized.error().message());
    return 1;
  }

  int exit_code = 1;
  coro::SpawnDetach(loop, ConnectOnce(loop, exit_code));

  std::println("connecting to 127.0.0.1:{}", kEchoPort);
  loop.Run(std::stop_token{});

  return exit_code;
}
