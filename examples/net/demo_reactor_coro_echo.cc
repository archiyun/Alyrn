// Copyright (c) 2026 Arsenova
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

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/io/async_listener.h"
#include "coropact/io/async_stream.h"
#include "coropact/io/stream_algorithms.h"
#include "coropact/net/event_loop.h"
#include "coropact/net/event_loop_scheduler.h"
#include "coropact/net/inet_address.h"
#include "coropact/net/reactor_listener.h"
#include "coropact/net/reactor_stream.h"

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

template <coropact::io::AsyncStream Stream>
[[maybe_unused]] coropact::coro::Task<void> EchoOnceSession(Stream stream) {
  std::array<std::byte, 4096> buffer{};

  auto result = co_await coropact::io::EchoOnce(stream, buffer);
  if (!result.has_value()) {
    std::cerr << "echo once failed: " << result.error().message() << '\n';
  }

  co_await stream.Close();
}

template <coropact::io::AsyncStream Stream>
coropact::coro::Task<void> Session(Stream stream, long long* active_sessions,
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
        co_await coropact::io::WriteAll(stream, Bytes(pending));
      }
      break;
    }

    pending.append(reinterpret_cast<const char*>(buffer.data()), n);
    if (pending.size() > 64 * 1024) {
      co_await coropact::io::WriteAll(stream, Bytes("ERR line too long\n"));
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
        co_await coropact::io::WriteAll(stream, Bytes("bye\n"));
        co_await stream.Close();
        --(*active_sessions);
        co_return;
      }

      if (command == "/stats") {
        std::string reply = "active_sessions=" + std::to_string(*active_sessions) +
                            " total_messages=" + std::to_string(*total_messages) + "\n";
        co_await coropact::io::WriteAll(stream, Bytes(reply));
        pending.erase(0, line_end + 1);
        continue;
      }

      ++(*total_messages);

      auto write_result = co_await coropact::io::WriteAll(stream, Bytes(line));
      if (!write_result.has_value()) {
        std::cerr << "write failed: " << write_result.error().message() << '\n';
        break;
      }

      pending.erase(0, line_end + 1);
    }
  }

  co_await stream.Close();
  --(*active_sessions);
}

template <coropact::io::AsyncListener Listener>
coropact::coro::Task<void> AcceptLoop(Listener* listener, coropact::coro::Scheduler* scheduler,
                                  long long* active_sessions, long long* total_messages) {
  using Stream = typename Listener::Stream;

  for (;;) {
    auto accepted = co_await listener->Accept();
    if (!accepted.has_value()) {
      std::cerr << "accept failed: " << accepted.error().message() << '\n';
      co_return;
    }

    coropact::coro::Spawn(*scheduler,
                      Session<Stream>(std::move(*accepted), active_sessions, total_messages))
        .Detach();

    // To start with the smallest possible demo, replace the Spawn above with:
    // coropact::coro::Spawn(*scheduler, EchoOnceSession<Stream>(std::move(*accepted))).Detach();
  }
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);

  const auto port = static_cast<std::uint16_t>(EnvInt("PORT", 9090));

  coropact::net::EventLoop loop;
  coropact::net::EventLoopScheduler scheduler(&loop);
  auto listener_result = coropact::net::ReactorListener::Create(&loop, coropact::net::InetAddress(port));
  if (!listener_result.has_value()) {
    std::cerr << "failed to create listener: " << listener_result.error().message() << '\n';
    return 1;
  }
  auto listener = std::move(*listener_result);

  long long active_sessions = 0;
  long long total_messages = 0;

  coropact::coro::Spawn(scheduler, AcceptLoop(&listener, &scheduler, &active_sessions, &total_messages))
      .Detach();

  std::cout << "reactor coro echo listening on 127.0.0.1:" << port << '\n';
  std::cout << "try: nc 127.0.0.1 " << port << '\n';

  loop.Loop();
  return 0;
}
