// SPDX-License-Identifier: MIT
//
// Task<T> is a lazy, move-only leaf coroutine. It is consumed exactly once by
// co_await and knows nothing about schedulers, spawn roots, or I/O backends.
#pragma once

#include <coroutine>
#include <utility>

#include "coropact/coro/detail/task_fwd.h"
#include "coropact/coro/detail/task_state.h"
#include "coropact/utils/macros.h"

namespace coropact::coro {

template <Returnable T>
class [[nodiscard]] Task {
public:
  COROPACT_DELETE_COPY(Task);

  using promise_type = detail::TaskPromise<T>;
  using Handle = std::coroutine_handle<promise_type>;

  Task() noexcept = default;
  explicit Task(Handle handle) noexcept : handle_(handle) {}

  Task(Task&& other) noexcept : handle_(other.Release()) {}
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      if (handle_) {
        handle_.destroy();
      }
      handle_ = other.Release();
    }
    return *this;
  }
  ~Task() {
    if (handle_) {
      handle_.destroy();
    }
  }

  auto operator co_await() && noexcept { return detail::TaskAwaiter<T>{Release()}; }

  Handle Release() noexcept { return std::exchange(handle_, {}); }
  explicit operator bool() const noexcept { return handle_ != nullptr; }

private:
  Handle handle_{};
};

namespace detail {

template <Returnable T>
Task<T> TaskPromise<T>::get_return_object() noexcept {
  return Task<T>{std::coroutine_handle<TaskPromise<T>>::from_promise(*this)};
}

inline Task<void> TaskPromise<void>::get_return_object() noexcept {
  return Task<void>{std::coroutine_handle<TaskPromise<void>>::from_promise(*this)};
}

}  // namespace detail
}  // namespace coropact::coro
