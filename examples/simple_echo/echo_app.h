// SPDX-License-Identifier: MIT
#pragma once

#include <array>
#include <cstddef>
#include <print>
#include <span>
#include <utility>

#include "alyrn/io.h"
#include "alyrn/spawn.h"

namespace simple_echo {

namespace {

constexpr std::size_t kDefaultBufferSize = 4096;

}  // namespace

template <alyrn::io::AsyncStream Stream>
auto EchoSession(Stream stream) -> alyrn::Task<alyrn::Result<void>> {
  std::array<std::byte, kDefaultBufferSize> buffer{};

  for (;;) {
    auto read = co_await stream.Read(buffer);
    if (!read.has_value()) {
      co_return std::unexpected(read.error());
    }
    if (*read == 0) {
      co_return {};
    }

    const std::span<const std::byte> payload(buffer.data(), *read);
    auto written = co_await stream.Write(payload);
    if (!written.has_value()) {
      co_return std::unexpected(written.error());
    }
  }
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
