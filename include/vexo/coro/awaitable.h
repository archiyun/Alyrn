// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: the Awaitable concept (duck-typed -- await_ready /
// await_suspend / await_resume, or a member operator co_await). It constrains
// and documents Spawn/SyncWait inputs. There is intentionally no await_transform
// anywhere: a future IO awaiter satisfies Awaitable directly and is co_awaited
// as-is, keeping the promise free of any IO concept.
#pragma once

#include <concepts>
#include <coroutine>
#include <type_traits>
#include <utility>

namespace vexo::coro {

template <class T>
concept Awaiter = requires(T& awaiter, std::coroutine_handle<> handle) {
                    { awaiter.await_ready() } -> std::convertible_to<bool>;
                    awaiter.await_suspend(handle);
                    awaiter.await_resume();
                  };

template <class T>
concept Awaitable =
    Awaiter<std::remove_reference_t<T>> || requires(T&& awaitable) {
                                             {
                                               std::forward<T>(awaitable).operator co_await()
                                               } -> Awaiter;
                                           };

}  // namespace vexo::coro
