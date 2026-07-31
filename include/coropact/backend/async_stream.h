// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <cstddef>
#include <span>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/awaitable.h"
#include "coropact/coro/task.h"
#include "coropact/net/read_into.h"
#include "coropact/net/write_part.h"

namespace coropact::backend {

// ReadSome() borrows buffer storage. The storage and its address must remain
// valid and stable until await_resume(). Every terminal path, including close
// and cancellation, reaches await_resume() only after the backend no longer
// accesses the borrowed storage.
template <class T>
concept AsyncReadStream = requires(T& stream, std::span<std::byte> buffer) {
  requires coro::Awaitable<decltype(stream.ReadSome(buffer))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.ReadSome(buffer))>,
                        base::Result<std::size_t>>;
};

// Optional owning-read extension. ReadInto() consumes a move-only Buffer and
// returns it on every terminal path together with the read status. It is kept
// separate from AsyncReadStream so existing adapters retain the small core
// transport interface.
template <class T>
concept AsyncOwnedReadStream = requires(T& stream, net::Buffer buffer, std::size_t reserve) {
  requires coro::Awaitable<decltype(stream.ReadInto(std::move(buffer), reserve))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.ReadInto(std::move(buffer), reserve))>,
                        net::ReadIntoOutcome>;
};

template <class T>
concept AsyncWriteStream =
    requires(T& stream, std::span<const std::byte> buffer) {
      requires coro::Awaitable<decltype(stream.WriteSome(buffer))>;
      requires std::same_as<
          coro::AwaitResult<decltype(stream.WriteSome(buffer))>,
          base::Result<std::size_t>>;
    };

template <class T>
concept AsyncClosableStream = requires(T& stream) {
  { stream.Shutdown() } -> std::same_as<coro::Task<base::Result<void>>>;
  { stream.Close() } -> std::same_as<coro::Task<base::Result<void>>>;
};

template <class T>
concept AsyncStream =
    AsyncReadStream<T> && AsyncWriteStream<T> && AsyncClosableStream<T>;

using WritePart = ::coropact::net::WritePart;

template <class T>
concept AsyncScatterWriteStream =
    AsyncWriteStream<T> &&
    requires(T& stream, std::span<const WritePart> buffers) {
      requires coro::Awaitable<decltype(stream.WriteSome(buffers))>;
      requires std::same_as<
          coro::AwaitResult<decltype(stream.WriteSome(buffers))>,
          base::Result<std::size_t>>;
    };

}  // namespace coropact::backend
