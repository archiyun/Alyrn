// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>

#include <concepts>
#include <optional>
#include <utility>

#include "coropact/coro/task.h"
#include "coropact/net/recv_source.h"

namespace coropact::backend {

using BufferLease = ::coropact::net::BufferLease;
using RecvEvent = ::coropact::net::RecvEvent;
using RecvSourceOptions = ::coropact::net::RecvSourceOptions;

namespace detail {

template <class Awaiter, class Result>
concept RecvAwaiter = requires(Awaiter&& awaiter, std::coroutine_handle<> continuation) {
  { std::forward<Awaiter>(awaiter).await_ready() } -> std::convertible_to<bool>;
  std::forward<Awaiter>(awaiter).await_suspend(continuation);
  { std::forward<Awaiter>(awaiter).await_resume() } -> std::same_as<Result>;
};

template <class Awaitable, class Result>
concept RecvAwaitable =
    RecvAwaiter<Awaitable, Result> ||
    requires(Awaitable&& awaitable) {
      requires RecvAwaiter<
          decltype(std::forward<Awaitable>(awaitable).operator co_await()), Result>;
    };

}  // namespace detail

template <class T>
concept AsyncRecvSource = requires(T& source) {
  typename T::Event;
  requires std::same_as<typename T::Event, RecvEvent>;
  requires detail::RecvAwaitable<
      decltype(source.Next()), base::Result<std::optional<typename T::Event>>>;
  { source.RequestStop() } -> std::same_as<base::Result<void>>;
  { source.Stop() } -> std::same_as<coro::Task<base::Result<void>>>;
};

}  // namespace coropact::backend
