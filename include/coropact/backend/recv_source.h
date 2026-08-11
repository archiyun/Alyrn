// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <optional>

#include "coropact/coro/awaitable.h"
#include "coropact/coro/task.h"
#include "coropact/net/recv_source.h"

namespace coropact::backend {

using BufferLease = ::coropact::net::BufferLease;
using RecvEvent = ::coropact::net::RecvEvent;
using RecvSourceOptions = ::coropact::net::RecvSourceOptions;

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

}  // namespace coropact::backend
