// SPDX-License-Identifier: MIT
#pragma once

// Shared adapter-contract header for epoll, uring, and kqueue. Include this
// file directly; there is no alyrn/backend.h. Applications use alyrn/io.h.

#include <concepts>
#include <optional>

#include "alyrn/backend/async_stream.h"
#include "alyrn/coro/awaitable.h"
#include "alyrn/net/accept_source.h"
#include "alyrn/result.h"
#include "alyrn/task.h"

namespace alyrn::backend {

using AcceptSourceOptions = net::AcceptSourceOptions;

template <class T>
concept AsyncAcceptSource = requires(T& source) {
  typename T::StreamType;
  requires AsyncStream<typename T::StreamType>;
  requires coro::Awaitable<decltype(source.Next())>;
  requires std::same_as<coro::AwaitResult<decltype(source.Next())>,
                        Result<std::optional<typename T::StreamType>>>;
  { source.Stop() } -> std::same_as<Task<Result<void>>>;
};

}  // namespace alyrn::backend
