// SPDX-License-Identifier: MIT
#pragma once

// Shared adapter-contract header for epoll and uring. Include this
// file directly; there is no alyrn/backend.h. Applications use alyrn/io.h.

#include <concepts>
#include <optional>

#include "alyrn/coro/awaitable.h"
#include "alyrn/net/recv_source.h"
#include "alyrn/result.h"
#include "alyrn/task.h"

namespace alyrn::backend {

using BufferLease = net::BufferLease;
using RecvEvent = net::RecvEvent;
using RecvSourceOptions = net::RecvSourceOptions;

template <class T>
concept AsyncRecvSource = requires(T& source) {
  typename T::Event;
  requires std::same_as<typename T::Event, RecvEvent>;
  requires coro::Awaitable<decltype(source.Next())>;
  requires std::same_as<coro::AwaitResult<decltype(source.Next())>,
                        Result<std::optional<typename T::Event>>>;
  { source.RequestStop() } -> std::same_as<Result<void>>;
  { source.Stop() } -> std::same_as<Task<Result<void>>>;
};

}  // namespace alyrn::backend
