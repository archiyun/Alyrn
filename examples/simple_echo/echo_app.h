// SPDX-License-Identifier: MIT
#pragma once

#include <array>
#include <cstddef>
#include <print>
#include <span>
#include <utility>

#include "alyrn/alyrn.h"
#include "alyrn/io.h"

namespace simple_echo {

using alyrn::Result;

template <alyrn::io::AsyncStream Stream>
auto EchoSession(Stream stream) -> alyrn::Task<Result<void>> {
  std::array<std::byte, 4096> buffer{};
  Result<void> session_result{};

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
    auto written = co_await stream.WriteAll(payload);
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

template <alyrn::io::AsyncStream Stream>
auto HandleConnection(Stream stream) -> alyrn::DetachedTask {
  auto result = co_await EchoSession(std::move(stream));
  if (!result.has_value()) {
    std::println(stderr, "session failed: {}", result.error().message());
  }
  co_return;
}

}  // namespace simple_echo
