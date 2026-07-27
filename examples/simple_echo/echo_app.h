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
auto EchoSession(Stream stream) -> DetachedTask {
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

    const std::span<const std::byte> payload(buffer.data(), *read);
    auto written = co_await WriteAll(stream, payload);
    if (!written.has_value()) {
      std::println(stderr, "read failed: {}", read.error().message());
      break;
    }
  }

  co_await stream.Close();
}

template <AsyncListener Listener>
auto AcceptLoop(Listener& listener, Scheduler& scheduler) -> DetachedTask {
  for (;;) {
    auto accepted = co_await listener.Accept();
    if (!accepted.has_value()) {
      std::println(stderr, "accept failed: {}", accepted.error().message());
      co_return;
    }

    SpawnDetach(scheduler, EchoSession(std::move(*accepted)));
  }
}

}  // namespace simple_echo
