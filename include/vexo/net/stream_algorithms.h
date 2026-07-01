// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <cstddef>
#include <expected>
#include <span>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/net/async_stream.h"

namespace vexo::net {

template <AsyncWriteStream Stream>
coro::Task<base::Result<void>> WriteAll(Stream& stream, std::span<const std::byte> buffer) {
  while (!buffer.empty()) {
    base::Result<std::size_t> result = co_await stream.WriteSome(buffer);
    if (!result.has_value()) {
      co_return std::unexpected(result.error());
    }
    if (*result == 0) {
      co_return std::unexpected(base::make_errno(EPIPE));
    }
    buffer = buffer.subspan(*result);
  }
  co_return base::Result<void>{};
}

template <AsyncStream Stream>
coro::Task<base::Result<void>> EchoOnce(Stream& stream, std::span<std::byte> buffer) {
  base::Result<std::size_t> nread = co_await stream.ReadSome(buffer);
  if (!nread.has_value()) {
    co_return std::unexpected(nread.error());
  }
  if (*nread == 0) {
    co_return base::Result<void>{};
  }

  base::Result<void> written = co_await WriteAll(stream, buffer.first(*nread));
  if (!written.has_value()) {
    co_return std::unexpected(written.error());
  }
  co_return base::Result<void>{};
}

}  // namespace vexo::net
