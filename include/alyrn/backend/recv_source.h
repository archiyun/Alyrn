// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <optional>

#include "alyrn/coro/awaitable.h"
#include "alyrn/coro/task.h"
#include "alyrn/net/recv_source.h"

namespace alyrn::backend {

using BufferLease = ::alyrn::net::BufferLease;
using RecvEvent = ::alyrn::net::RecvEvent;
using RecvSourceOptions = ::alyrn::net::RecvSourceOptions;

template <class T>
concept AsyncRecvSource = requires(T& source) {
  typename T::Event;
  requires std::same_as<typename T::Event, RecvEvent>;
  requires coro::Awaitable<decltype(source.Next())>;
  requires std::same_as<coro::AwaitResult<decltype(source.Next())>,
                        Result<std::optional<typename T::Event>>>;
  { source.RequestStop() } -> std::same_as<Result<void>>;
  { source.Stop() } -> std::same_as<coro::Task<Result<void>>>;
};

}  // namespace alyrn::backend
