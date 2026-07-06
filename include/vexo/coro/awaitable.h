// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Backend-neutral awaitable concepts. This header only describes the language
// shape of awaiters/awaitables; it does not know about schedulers, EventLoop, or
// any I/O backend.
#pragma once

#include <concepts>
#include <coroutine>
#include <utility>

namespace vexo::coro {

template <class T>
concept Awaiter = requires(T awaiter, std::coroutine_handle<> handle) {
  { awaiter.await_ready() } -> std::convertible_to<bool>;
  awaiter.await_suspend(handle);
  awaiter.await_resume();
};

template <class T>
concept MemberCoAwaitable = requires {
  requires Awaiter<decltype(std::declval<T>().operator co_await())>;
};

template <class T>
concept Awaitable = Awaiter<T> || MemberCoAwaitable<T>;

}  // namespace vexo::coro
