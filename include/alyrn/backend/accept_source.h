// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <optional>

#include "alyrn/backend/async_stream.h"
#include "alyrn/coro/awaitable.h"
#include "alyrn/coro/task.h"
#include "alyrn/net/accept_source.h"

namespace alyrn::backend {

using AcceptSourceOptions = ::alyrn::net::AcceptSourceOptions;

template <class T>
concept AsyncAcceptSource = requires(T& source) {
  typename T::StreamType;
  requires AsyncStream<typename T::StreamType>;
  requires coro::Awaitable<decltype(source.Next())>;
  requires std::same_as<coro::AwaitResult<decltype(source.Next())>,
                        Result<std::optional<typename T::StreamType>>>;
  { source.Stop() } -> std::same_as<coro::Task<Result<void>>>;
};

}  // namespace alyrn::backend
