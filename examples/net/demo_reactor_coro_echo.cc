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

#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/spawn.h"
#include "vexo/coro/task.h"
#include "vexo/io/async_listener.h"
#include "vexo/io/async_stream.h"
#include "vexo/io/stream_algorithms.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/event_loop_scheduler.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/reactor_listener.h"
#include "vexo/net/reactor_stream.h"

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

template <vexo::io::AsyncStream Stream>
[[maybe_unused]] vexo::coro::Task<void> EchoOnceSession(std::unique_ptr<Stream> stream) {
  std::array<std::byte, 4096> buffer{};

  auto result = co_await vexo::io::EchoOnce(*stream, buffer);
  if (!result.has_value()) {
    std::cerr << "echo once failed: " << result.error().message() << '\n';
  }

  co_await stream->Close();
}

template <vexo::io::AsyncStream Stream>
vexo::coro::Task<void> Session(std::unique_ptr<Stream> stream, long long* active_sessions,
                               long long* total_messages) {
  ++(*active_sessions);

  std::array<std::byte, 4096> buffer{};
  std::string pending;

  for (;;) {
    auto read_result = co_await stream->ReadSome(buffer);
    if (!read_result.has_value()) {
      std::cerr << "read failed: " << read_result.error().message() << '\n';
      break;
    }

    const std::size_t n = *read_result;
    if (n == 0) {
      if (!pending.empty()) {
        ++(*total_messages);
        co_await vexo::io::WriteAll(*stream, Bytes(pending));
      }
      break;
    }

    pending.append(reinterpret_cast<const char*>(buffer.data()), n);
    if (pending.size() > 64 * 1024) {
      co_await vexo::io::WriteAll(*stream, Bytes("ERR line too long\n"));
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
        co_await vexo::io::WriteAll(*stream, Bytes("bye\n"));
        co_await stream->Close();
        --(*active_sessions);
        co_return;
      }

      if (command == "/stats") {
        std::string reply = "active_sessions=" + std::to_string(*active_sessions) +
                            " total_messages=" + std::to_string(*total_messages) + "\n";
        co_await vexo::io::WriteAll(*stream, Bytes(reply));
        pending.erase(0, line_end + 1);
        continue;
      }

      ++(*total_messages);

      auto write_result = co_await vexo::io::WriteAll(*stream, Bytes(line));
      if (!write_result.has_value()) {
        std::cerr << "write failed: " << write_result.error().message() << '\n';
        break;
      }

      pending.erase(0, line_end + 1);
    }
  }

  co_await stream->Close();
  --(*active_sessions);
}

template <vexo::io::AsyncListener Listener>
vexo::coro::Task<void> AcceptLoop(Listener* listener, vexo::coro::Scheduler* scheduler,
                                  long long* active_sessions, long long* total_messages) {
  using Stream = typename Listener::Stream;

  for (;;) {
    auto accepted = co_await listener->Accept();
    if (!accepted.has_value()) {
      std::cerr << "accept failed: " << accepted.error().message() << '\n';
      co_return;
    }

    vexo::coro::Spawn(*scheduler,
                      Session<Stream>(std::move(*accepted), active_sessions, total_messages))
        .Detach();

    // To start with the smallest possible demo, replace the Spawn above with:
    // vexo::coro::Spawn(*scheduler, EchoOnceSession<Stream>(std::move(*accepted))).Detach();
  }
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);

  const auto port = static_cast<std::uint16_t>(EnvInt("PORT", 9090));

  vexo::net::EventLoop loop;
  vexo::net::EventLoopScheduler scheduler(&loop);
  vexo::net::ReactorListener listener(&loop, vexo::net::InetAddress(port));

  long long active_sessions = 0;
  long long total_messages = 0;

  vexo::coro::Spawn(scheduler, AcceptLoop(&listener, &scheduler, &active_sessions, &total_messages))
      .Detach();

  std::cout << "reactor coro echo listening on 127.0.0.1:" << port << '\n';
  std::cout << "try: nc 127.0.0.1 " << port << '\n';

  loop.Loop();
  return 0;
}
