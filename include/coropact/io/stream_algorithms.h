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

template <AsyncReadStream Stream>
auto ReadSome(Stream& stream, Buffer& buffer, std::size_t reserve = 4096)
    -> coro::Task<base::Result<std::size_t>> {
  if constexpr (requires { stream.ReadSome(buffer, reserve); }) {
    co_return co_await stream.ReadSome(buffer, reserve);
  }

  auto iovs = buffer.PrepareWrite(reserve, 1);
  if (iovs.empty()) {
    co_return std::unexpected(base::MakeErrno(ENOMEM));
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

}  // namespace coropact::io
