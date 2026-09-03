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
    if (!read.HasValue()) {
      co_return std::unexpected(read.Error());
    }
    if (*read == 0) {
      co_return {};
    }

    const std::span<const std::byte> payload(buffer.data(), *read);
    auto written = co_await stream.Write(payload);
    if (!written.HasValue()) {
      co_return std::unexpected(written.Error());
    }
  }
}

template <alyrn::io::AsyncStream Stream>
auto EchoSession2(Stream stream) -> alyrn::Task<alyrn::Result<void>> {
  std::array<std::byte, kDefaultBufferSize> buffer{};

  for (;;) {
    // Value() eagerly unwraps Result, like Rust's unwrap(). A failed read
    // triggers Panic and terminates the process, so normal production I/O
    // should use EchoSession's explicit error propagation instead.
    auto read_bytes = (co_await stream.Read(buffer)).Value();
    if (read_bytes == 0) {  // EOF
      co_return {};
    }

    auto payload = std::span{buffer.data(), read_bytes};
    // Expect() also triggers Panic and terminates the process, but lets the
    // caller add failure context. It is for assertion-like paths, not
    // recoverable I/O failures.
    (co_await stream.Write(payload)).Expect("echo write failed");
  }
}

template <alyrn::io::AsyncStream Stream>
auto HandleConnection(Stream stream) -> alyrn::DetachedTask {
  auto result = co_await EchoSession(std::move(stream));
  if (!result.HasValue()) {
    std::println(stderr, "session failed: {}", result.Error().message());
  }
  co_return;
}

}  // namespace simple_echo
