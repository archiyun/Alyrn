// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <cstddef>
#include <span>

#include "coropact/base/error.h"
#include "coropact/coro/awaitable.h"
#include "coropact/coro/task.h"

namespace coropact::io {

template <class T>
concept AsyncReadStream = requires(T& stream, std::span<std::byte> buffer) {
  requires coro::Awaitable<decltype(stream.ReadSome(buffer))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.ReadSome(buffer))>,
                        base::Result<std::size_t>>;
};

template <class T>
concept AsyncWriteStream = requires(T& stream, std::span<const std::byte> buffer) {
  requires coro::Awaitable<decltype(stream.WriteSome(buffer))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.WriteSome(buffer))>,
                        base::Result<std::size_t>>;
};

template <class T>
concept AsyncClosableStream = requires(T& stream) {
  { stream.Shutdown() } -> std::same_as<coro::Task<base::Result<void>>>;
  { stream.Close() } -> std::same_as<coro::Task<base::Result<void>>>;
};

template <class T>
concept AsyncStream = AsyncReadStream<T> && AsyncWriteStream<T> && AsyncClosableStream<T>;

}  // namespace coropact::io
