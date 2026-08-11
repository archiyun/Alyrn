// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <cstddef>
#include <span>
#include <utility>

#include "coropact/result.h"
#include "coropact/coro/awaitable.h"
#include "coropact/coro/task.h"
#include "coropact/net/read_into.h"
#include "coropact/time/clock.h"

namespace coropact::backend {

// ReadSome() borrows buffer storage. The storage and its address must remain
// valid and stable until await_resume(). Every terminal path, including close
// and cancellation, reaches await_resume() only after the backend no longer
// accesses the borrowed storage.
template <class T>
concept AsyncReadStream = requires(T& stream, std::span<std::byte> buffer) {
  requires coro::Awaitable<decltype(stream.ReadSome(buffer))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.ReadSome(buffer))>,
                        Result<std::size_t>>;
};

// Optional timed-read extension. It deliberately stays outside AsyncStream:
// a timeout changes physical convergence from one execution path to a
// composite read-and-timeout protocol. A profile bit alone never adds this
// C++ interface or its lifetime guarantee.
template <class T>
concept AsyncTimedReadStream =
    AsyncReadStream<T> && requires(T& stream, std::span<std::byte> buffer, time::Duration timeout) {
      requires coro::Awaitable<decltype(stream.ReadSomeFor(buffer, timeout))>;
      requires std::same_as<coro::AwaitResult<decltype(stream.ReadSomeFor(buffer, timeout))>,
                            Result<std::size_t>>;
    };

// Optional ReadInto extension. ReadInto() consumes a move-only Buffer and
// returns it on every terminal path together with the read status. It is kept
// separate from AsyncReadStream so existing adapters retain the small core
// transport interface.
template <class T>
concept AsyncReadIntoStream = requires(T& stream, net::Buffer buffer, std::size_t reserve) {
  requires coro::Awaitable<decltype(stream.ReadInto(std::move(buffer), reserve))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.ReadInto(std::move(buffer), reserve))>,
                        net::ReadIntoOutcome>;
};

// WriteAll() keeps the input storage borrowed until every byte has either
// been accepted by the backend or the operation reaches a terminal error.
// It is the core write contract; each backend keeps short-write progress and
// resource-lifetime handling private to its implementation.
template <class T>
concept AsyncWriteStream = requires(T& stream, std::span<const std::byte> buffer) {
  requires coro::Awaitable<decltype(stream.WriteAll(buffer))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.WriteAll(buffer))>, Result<void>>;
};

template <class T>
concept AsyncClosableStream = requires(T& stream) {
  { stream.Shutdown() } -> std::same_as<coro::Task<Result<void>>>;
  { stream.Close() } -> std::same_as<coro::Task<Result<void>>>;
};

template <class T>
concept AsyncStream = AsyncReadStream<T> && AsyncWriteStream<T> && AsyncClosableStream<T>;

// A complete stream with the optional timed-read semantic extension.
template <class T>
concept AsyncTimedStream = AsyncStream<T> && AsyncTimedReadStream<T>;

}  // namespace coropact::backend
