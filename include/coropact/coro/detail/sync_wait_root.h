// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <semaphore>
#include <type_traits>
#include <utility>

#include "coropact/coro/frame_allocator.h"
#include "coropact/coro/task.h"

namespace coropact::coro::detail {

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

}  // namespace coropact::coro::detail
