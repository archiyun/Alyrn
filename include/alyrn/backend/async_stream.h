// SPDX-License-Identifier: MIT
#pragma once

// Shared adapter-contract header for epoll, uring, and kqueue. Include this
// file directly; there is no alyrn/backend.h. Applications use alyrn/io.h.

#include <concepts>
#include <cstddef>
#include <span>
#include <utility>

#include "alyrn/coro/awaitable.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/read_into.h"
#include "alyrn/result.h"
#include "alyrn/task.h"

namespace alyrn::backend {

template <class T>
concept AsyncReadStream = requires(T& stream, std::span<std::byte> buffer) {
  requires coro::Awaitable<decltype(stream.ReadSome(buffer))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.ReadSome(buffer))>,
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

}  // namespace alyrn::backend
