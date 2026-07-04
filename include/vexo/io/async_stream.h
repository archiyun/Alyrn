// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <cstddef>
#include <span>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"

namespace vexo::io {

template <class T>
concept AsyncReadStream = requires(T& stream, std::span<std::byte> buffer) {
  { stream.ReadSome(buffer) } -> std::same_as<coro::Task<base::Result<std::size_t>>>;
};

template <class T>
concept AsyncWriteStream = requires(T& stream, std::span<const std::byte> buffer) {
  { stream.WriteSome(buffer) } -> std::same_as<coro::Task<base::Result<std::size_t>>>;
};

template <class T>
concept AsyncClosableStream = requires(T& stream) {
  { stream.Shutdown() } -> std::same_as<coro::Task<base::Result<void>>>;
  { stream.Close() } -> std::same_as<coro::Task<base::Result<void>>>;
};

template <class T>
concept AsyncStream = AsyncReadStream<T> && AsyncWriteStream<T> && AsyncClosableStream<T>;

}  // namespace vexo::io
