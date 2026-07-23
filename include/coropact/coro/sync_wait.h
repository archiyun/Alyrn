// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: SyncWait(Task<T>) -> T. An eager root drives the task
// and blocks the current thread on a std::binary_semaphore until it completes.
// For tests and pure-computation coroutines only -- never on a connection/IO
// path, where blocking the thread would stall the loop.
#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <semaphore>
#include <type_traits>
#include <utility>

#include "coropact/coro/task.h"

namespace coropact::coro {
namespace detail {

// Eager root (initial_suspend = suspend_never): begins running the moment it is
// created, co_awaits the lazy task, and self-destructs at final suspend.
struct SyncWaitRoot {
  struct promise_type : FrameAllocationSupport {
    SyncWaitRoot get_return_object() const noexcept { return {}; }
    std::suspend_never initial_suspend() const noexcept { return {}; }
    std::suspend_never final_suspend() const noexcept { return {}; }
    void return_void() const noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };
};

template <Returnable T>
  requires(!std::is_void_v<T>)
SyncWaitRoot SyncWaitRun(Task<T> task, std::optional<T>* out, std::binary_semaphore* done) {
  out->emplace(co_await std::move(task));
  done->release();
}

inline SyncWaitRoot SyncWaitRun(Task<void> task, std::binary_semaphore* done) {
  co_await std::move(task);
  done->release();
}

}  // namespace detail

template <Returnable T>
  requires(!std::is_void_v<T>)
[[nodiscard]] T SyncWait(Task<T> task) {
  std::optional<T> out;
  std::binary_semaphore done{0};
  detail::SyncWaitRun(std::move(task), &out, &done);
  done.acquire();
  return std::move(*out);
}

inline void SyncWait(Task<void> task) {
  std::binary_semaphore done{0};
  detail::SyncWaitRun(std::move(task), &done);
  done.acquire();
}

}  // namespace coropact::coro
