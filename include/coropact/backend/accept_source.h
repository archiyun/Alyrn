// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <optional>

#include "coropact/backend/async_stream.h"
#include "coropact/coro/awaitable.h"
#include "coropact/coro/task.h"
#include "coropact/net/accept_source.h"

namespace coropact::backend {

using AcceptSourceOptions = ::coropact::net::AcceptSourceOptions;

template <class T>
concept AsyncAcceptSource = requires(T& source) {
  typename T::StreamType;
  requires AsyncStream<typename T::StreamType>;
  requires coro::Awaitable<decltype(source.Next())>;
  requires std::same_as<coro::AwaitResult<decltype(source.Next())>,
                        Result<std::optional<typename T::StreamType>>>;
  { source.Stop() } -> std::same_as<coro::Task<Result<void>>>;
};

}  // namespace coropact::backend
