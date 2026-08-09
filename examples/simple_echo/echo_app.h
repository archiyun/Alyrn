// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <array>
#include <cstddef>
#include <print>
#include <span>
#include <utility>

#include "coropact/coro.h"
#include "coropact/io.h"

namespace simple_echo {

using namespace coropact::io;
using namespace coropact::coro;

template <AsyncStream Stream>
auto EchoSession(Stream stream) -> Task<coropact::base::Result<void>> {
  std::array<std::byte, 4096> buffer{};
  coropact::base::Result<void> session_result{};

  for (;;) {
    auto read = co_await stream.ReadSome(buffer);
    if (!read.has_value()) {
      session_result = std::unexpected(read.error());
      break;
    }
    if (*read == 0) {
      break;
    }

    const std::span<const std::byte> payload(buffer.data(), *read);
    auto written = co_await WriteAll(stream, payload);
    if (!written.has_value()) {
      session_result = std::unexpected(written.error());
      break;
    }
  }

  auto closed = co_await stream.Close();
  if (!closed.has_value()) {
    if (session_result.has_value()) {
      session_result = std::unexpected(closed.error());
    } else {
      std::println(stderr, "close failed: {}", closed.error().message());
    }
  }
  co_return session_result;
}

inline auto ObserveSession(JoinHandle<coropact::base::Result<void>> handle)
    -> DetachedTask {
  auto result = co_await std::move(handle);
  if (!result.has_value()) {
    std::println(stderr, "session failed: {}", result.error().message());
  }
  co_return;
}

template <AsyncListener Listener>
auto AcceptLoop(Listener& listener, Scheduler& scheduler) -> DetachedTask {
  for (;;) {
    auto accepted = co_await listener.Accept();
    if (!accepted.has_value()) {
      std::println(stderr, "accept failed: {}", accepted.error().message());
      co_return;
    }

    auto session = Spawn(scheduler, EchoSession(std::move(*accepted)));
    SpawnDetach(scheduler, ObserveSession(std::move(session)));
  }
}

}  // namespace simple_echo
