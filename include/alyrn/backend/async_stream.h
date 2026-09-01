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
#include "alyrn/net/recv.h"
#include "alyrn/result.h"
#include "alyrn/task.h"

namespace alyrn::backend {

template <class T>
concept AsyncReadStream = requires(T& stream, std::span<std::byte> buffer) {
  requires coro::Awaitable<decltype(stream.Read(buffer))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.Read(buffer))>, Result<std::size_t>>;
};

template <class T>
concept AsyncRecvStream = requires(T& stream, net::Buffer buffer, std::size_t reserve) {
  requires coro::Awaitable<decltype(stream.Recv(std::move(buffer), reserve))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.Recv(std::move(buffer), reserve))>,
                        net::RecvOutcome>;
};

template <class T>
concept AsyncRecvCopyStream = requires(T& stream) {
  requires coro::Awaitable<decltype(stream.Recv())>;
  requires std::same_as<coro::AwaitResult<decltype(stream.Recv())>, Result<net::Buffer>>;
};

template <class T>
concept AsyncWriteStream = requires(T& stream, std::span<const std::byte> buffer) {
  requires coro::Awaitable<decltype(stream.Write(buffer))>;
  requires std::same_as<coro::AwaitResult<decltype(stream.Write(buffer))>, Result<void>>;
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
