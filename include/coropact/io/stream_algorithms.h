// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <cstddef>
#include <expected>
#include <span>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/io/async_stream.h"
#include "coropact/io/buffer.h"

namespace coropact::io {

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

template <AsyncReadStream Stream>
coro::Task<base::Result<std::size_t>> ReadSome(Stream& stream, Buffer& buffer,
                                               std::size_t reserve = 4096) {
  if constexpr (requires { stream.ReadSome(buffer, reserve); }) {
    co_return co_await stream.ReadSome(buffer, reserve);
  }

  auto iovs = buffer.PrepareWrite(reserve, 1);
  if (iovs.empty()) {
    co_return std::unexpected(base::make_errno(ENOMEM));
  }

  auto writable = std::span<std::byte>(static_cast<std::byte*>(iovs[0].iov_base), iovs[0].iov_len);

  auto result = co_await stream.ReadSome(writable);
  if (!result.has_value()) {
    buffer.AbortWrite();
    co_return std::unexpected(result.error());
  }

  buffer.CommitWrite(*result);
  co_return *result;
}

template <AsyncWriteStream Stream>
coro::Task<base::Result<void>> WriteAll(Stream& stream, Buffer& buffer) {
  if constexpr (requires { stream.WriteSome(buffer); }) {
    while (!buffer.Empty()) {
      auto result = co_await stream.WriteSome(buffer);
      if (!result.has_value()) {
        co_return std::unexpected(result.error());
      }
      if (*result == 0) {
        co_return std::unexpected(base::make_errno(EPIPE));
      }
    }

    co_return base::Result<void>{};
  }

  while (!buffer.Empty()) {
    auto view = buffer.ContiguousView();
    auto result = co_await WriteAll(stream, view);
    if (!result.has_value()) {
      co_return std::unexpected(result.error());
    }
    buffer.Drain(view.size());
  }

  co_return base::Result<void>{};
}

}  // namespace coropact::io
