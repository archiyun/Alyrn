// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <cstddef>
#include <span>
#include <utility>

#include "alyrn/coro/awaitable.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/read_into.h"
#include "alyrn/result.h"
#include "alyrn/task.h"
#include "alyrn/time/clock.h"

namespace alyrn::io {

// ReadSome() borrows buffer storage until await_resume().  This is the
// backend-neutral application contract; concrete adapters may use readiness
// or completion events behind it.
template <class T>
concept AsyncReadStream = requires(T& stream, std::span<std::byte> buffer) {
  requires coro::Awaitable<decltype(stream.ReadSome(buffer))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.ReadSome(buffer))>,
                        Result<std::size_t>>;
};

template <class T>
concept AsyncTimedReadStream =
    AsyncReadStream<T> && requires(T& stream, std::span<std::byte> buffer, time::Duration timeout) {
      requires coro::Awaitable<decltype(stream.ReadSomeFor(buffer, timeout))>;
      requires std::same_as<coro::AwaitResult<decltype(stream.ReadSomeFor(buffer, timeout))>,
                            Result<std::size_t>>;
    };

template <class T>
concept AsyncReadIntoStream = requires(T& stream, net::Buffer buffer, std::size_t reserve) {
  requires coro::Awaitable<decltype(stream.ReadInto(std::move(buffer), reserve))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.ReadInto(std::move(buffer), reserve))>,
                        net::ReadIntoOutcome>;
};

template <class T>
concept AsyncWriteStream = requires(T& stream, std::span<const std::byte> buffer) {
  requires coro::Awaitable<decltype(stream.WriteAll(buffer))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.WriteAll(buffer))>, Result<void>>;
};

template <class T>
concept AsyncClosableStream = requires(T& stream) {
  { stream.Shutdown() } -> std::same_as<Task<Result<void>>>;
  { stream.Close() } -> std::same_as<Task<Result<void>>>;
};

template <class T>
concept AsyncStream =
    AsyncReadStream<T> && AsyncWriteStream<T> && AsyncClosableStream<T> && requires(T& stream) {
      { stream.LocalAddr() } -> std::same_as<Result<net::Endpoint>>;
      { stream.RemoteAddr() } -> std::same_as<const net::Endpoint&>;
      { stream.CloseRead() } -> std::same_as<Task<Result<void>>>;
      { stream.CloseWrite() } -> std::same_as<Task<Result<void>>>;
    };

template <class T>
concept AsyncTimedStream = AsyncStream<T> && AsyncTimedReadStream<T>;

}  // namespace alyrn::io
