// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "alyrn/base/check.h"
#include "alyrn/coro/detail/promise_base.h"
#include "alyrn/coro/detail/task_fwd.h"
#include "alyrn/utils/macros.h"

namespace alyrn::coro::detail {

template <Returnable T>
class TaskPromise final : public PromiseBase {
public:
  ALYRN_DELETE_COPY_MOVE(TaskPromise);

  TaskPromise() noexcept {}
  ~TaskPromise() {
    if (has_value_) {
      value_.~T();
    }
  }

  Task<T> get_return_object() noexcept;

  void return_value(T value) noexcept(std::is_nothrow_move_constructible_v<T>) {
    ALYRN_CHECK(!has_value_, "TaskPromise value was stored twice");
    ::new (static_cast<void*>(std::addressof(value_))) T(std::move(value));
    has_value_ = true;
  }

  T TakeValue() noexcept(std::is_nothrow_move_constructible_v<T>) {
    ALYRN_CHECK(has_value_, "TaskPromise value was taken before completion");
    return std::move(value_);
  }

private:
  union {
    T value_;
  };
  bool has_value_{false};
};

template <>
class TaskPromise<void> final : public PromiseBase {
public:
  ALYRN_DELETE_COPY_MOVE(TaskPromise);
  TaskPromise() = default;
  ~TaskPromise() = default;
  Task<void> get_return_object() noexcept;
  void return_void() const noexcept {}
};

// Owns the awaited child frame, transfers symmetrically into it, and destroys
// it after the result has been taken on resume.
template <Returnable T>
class TaskAwaiter {
public:
  using Promise = TaskPromise<T>;
  using Handle = std::coroutine_handle<Promise>;

  explicit TaskAwaiter(Handle handle) noexcept : handle_(handle) {
    ALYRN_CHECK(handle_, "cannot co_await an empty Task");
  }
  ALYRN_DELETE_COPY(TaskAwaiter);
  TaskAwaiter(TaskAwaiter&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  TaskAwaiter& operator=(TaskAwaiter&&) = delete;
  ~TaskAwaiter() {
    if (handle_) {
      handle_.destroy();
    }
  }

  bool await_ready() const noexcept { return false; }

  Handle await_suspend(std::coroutine_handle<> caller) noexcept {
    ALYRN_CHECK(handle_, "TaskAwaiter lost its child coroutine handle");
    handle_.promise().SetContinuation(caller);
    return handle_;
  }

  decltype(auto) await_resume() noexcept(std::is_void_v<T> ||
                                         std::is_nothrow_move_constructible_v<T>) {
    Handle completed = std::exchange(handle_, {});
    if constexpr (std::is_void_v<T>) {
      completed.destroy();
      return;
    } else {
      T value = completed.promise().TakeValue();
      completed.destroy();
      return value;
    }
  }

private:
  Handle handle_{};
};

}  // namespace alyrn::coro::detail
