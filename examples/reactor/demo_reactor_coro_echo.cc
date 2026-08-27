// SPDX-License-Identifier: MIT
//
// Coroutine echo demo with backend-neutral business code.
//
// Build:
//   cmake --build build --target demo_reactor_coro_echo -j"$(nproc)"
//
// Run:
//   PORT=9090 ./build/examples/net/demo_reactor_coro_echo
//
// Try:
//   nc 127.0.0.1 9090
//   hello
//   /stats
//   /quit

#include <array>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "alyrn/result.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/io/async_listener.h"
#include "alyrn/io/async_stream.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/reactor/listener.h"
#include "alyrn/reactor/loop.h"
#include "alyrn/reactor/stream.h"

namespace {

std::span<const std::byte> Bytes(std::string_view text) {
  return std::as_bytes(std::span<const char>(text.data(), text.size()));
}

int EnvInt(const char* key, int fallback) {
  const char* value = std::getenv(key);
  return value ? std::atoi(value) : fallback;
}

std::string_view StripLineEnding(std::string_view line) {
  if (!line.empty() && line.back() == '\n') {
    line.remove_suffix(1);
  }
  if (!line.empty() && line.back() == '\r') {
    line.remove_suffix(1);
  }
  return line;
}

template <alyrn::io::AsyncStream Stream>
alyrn::coro::DetachedTask Session(Stream stream, long long* active_sessions,
                                     long long* total_messages) {
  ++(*active_sessions);

  std::array<std::byte, 4096> buffer{};
  std::string pending;

  for (;;) {
    auto read_result = co_await stream.ReadSome(buffer);
    if (!read_result.has_value()) {
      std::cerr << "read failed: " << read_result.error().message() << '\n';
      break;
    }

    const std::size_t n = *read_result;
    if (n == 0) {
      if (!pending.empty()) {
        ++(*total_messages);
        (void)co_await stream.WriteAll(Bytes(pending));
      }
      break;
    }

    pending.append(reinterpret_cast<const char*>(buffer.data()), n);
    if (pending.size() > 64 * 1024) {
      (void)co_await stream.WriteAll(Bytes("ERR line too long\n"));
      break;
    }

    for (;;) {
      const std::size_t line_end = pending.find('\n');
      if (line_end == std::string::npos) {
        break;
      }

      const std::string_view line(pending.data(), line_end + 1);
      const std::string_view command = StripLineEnding(line);

      if (command == "/quit") {
        (void)co_await stream.WriteAll(Bytes("bye\n"));
        (void)co_await stream.Close();
        --(*active_sessions);
        co_return;
      }

      if (command == "/stats") {
        std::string reply = "active_sessions=" + std::to_string(*active_sessions) +
                            " total_messages=" + std::to_string(*total_messages) + "\n";
        (void)co_await stream.WriteAll(Bytes(reply));
        pending.erase(0, line_end + 1);
        continue;
      }

      ++(*total_messages);

      auto write_result = co_await stream.WriteAll(Bytes(line));
      if (!write_result.has_value()) {
        std::cerr << "write failed: " << write_result.error().message() << '\n';
        break;
      }

      pending.erase(0, line_end + 1);
    }
  }

  (void)co_await stream.Close();
  --(*active_sessions);
}

template <alyrn::io::AsyncListener Listener>
alyrn::coro::DetachedTask AcceptLoop(Listener* listener, alyrn::reactor::Loop* scheduler,
                                        long long* active_sessions, long long* total_messages) {
  using Stream = typename Listener::StreamType;

  for (;;) {
    auto accepted = co_await listener->Accept();
    if (!accepted.has_value()) {
      std::cerr << "accept failed: " << accepted.error().message() << '\n';
      co_return;
    }

    alyrn::coro::SpawnDetach(
        *scheduler, Session<Stream>(std::move(*accepted), active_sessions, total_messages));
  }
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);

  const auto port = static_cast<std::uint16_t>(EnvInt("PORT", 9090));

  alyrn::reactor::Loop loop;
  auto listener_result =
      alyrn::reactor::Listener::Create(&loop, alyrn::net::Endpoint(port));
  if (!listener_result.has_value()) {
    std::cerr << "failed to create listener: " << listener_result.error().message() << '\n';
    return 1;
  }
  auto listener = std::move(*listener_result);

  long long active_sessions = 0;
  long long total_messages = 0;

  alyrn::coro::SpawnDetach(loop,
                              AcceptLoop(&listener, &loop, &active_sessions, &total_messages));

  std::cout << "reactor coro echo listening on 127.0.0.1:" << port << '\n';
  std::cout << "try: nc 127.0.0.1 " << port << '\n';

  loop.Run();
  return 0;
}
