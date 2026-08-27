// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <optional>

#include "alyrn/coro/awaitable.h"
#include "alyrn/io/async_stream.h"
#include "alyrn/net/accept_source.h"
#include "alyrn/result.h"
#include "alyrn/task.h"

namespace alyrn::io {

using AcceptSourceOptions = ::alyrn::net::AcceptSourceOptions;

template <class T>
concept AsyncAcceptSource = requires(T& source) {
  typename T::StreamType;
  requires AsyncStream<typename T::StreamType>;
  requires coro::Awaitable<decltype(source.Next())>;
  requires std::same_as<coro::AwaitResult<decltype(source.Next())>,
                        Result<std::optional<typename T::StreamType>>>;
  { source.Stop() } -> std::same_as<Task<Result<void>>>;
};

}  // namespace alyrn::io
