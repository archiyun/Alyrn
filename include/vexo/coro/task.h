// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: Task<T>, the lazy leaf coroutine that is consumed by
// co_await. It owns its frame (move-only, awaited exactly once), transfers
// symmetrically into a child on co_await, and stores its return value with
// manual lifetime so a non-default-constructible T works. No spawn, join, or
// scheduling logic lives here -- Task knows nothing about how it is driven.
#pragma once

#include <cassert>
#include <concepts>
#include <coroutine>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "vexo/coro/detail/promise_base.h"
#include "vexo/utils/macros.h"

namespace vexo::coro {

// A Task yields void or any movable object. Non-void values live in the frame,
// so they must be move-constructible; void is handled by a dedicated promise.
template <class T>
concept Returnable =
    std::is_void_v<T> || (std::is_object_v<T> && !std::is_array_v<T> && std::move_constructible<T>);

template <Returnable T = void>
class Task;

namespace detail {

template <Returnable T>
class TaskPromise final : public PromiseBase {
public:
  VEXO_DELETE_COPY_MOVE(TaskPromise);
  TaskPromise() noexcept {}
  ~TaskPromise() {
    if (has_value_) {
      value_.~T();
    }
  }

  Task<T> get_return_object() noexcept;

  void return_value(T value) noexcept(std::is_nothrow_move_constructible_v<T>) {
    assert(!has_value_);
    ::new (static_cast<void*>(std::addressof(value_))) T(std::move(value));
    has_value_ = true;
  }

  // Precondition: the coroutine reached co_return. Moves the stored value out.
  T TakeValue() noexcept(std::is_nothrow_move_constructible_v<T>) {
    assert(has_value_);
    return std::move(value_);
  }

private:
  // Manual union storage: the value is constructed in place by return_value and
  // destroyed once here. A union avoids requiring T to be default-constructible.
  union {
    T value_;
  };
  bool has_value_{false};
};

template <>
class TaskPromise<void> final : public PromiseBase {
public:
  VEXO_DELETE_COPY_MOVE(TaskPromise);
  TaskPromise() = default;
  ~TaskPromise() = default;
  Task<void> get_return_object() noexcept;
  void return_void() const noexcept {}
};

// Owns the awaited child frame, transfers symmetrically into it, and destroys it
// after the value has been taken on resume.
template <Returnable T>
class TaskAwaiter {
public:
  using Promise = TaskPromise<T>;
  using Handle = std::coroutine_handle<Promise>;

  explicit TaskAwaiter(Handle handle) noexcept : handle_(handle) {
    assert(handle_ && "cannot co_await an empty Task.");
  }
  VEXO_DELETE_COPY(TaskAwaiter);
  TaskAwaiter(TaskAwaiter&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  TaskAwaiter& operator=(TaskAwaiter&&) = delete;
  ~TaskAwaiter() {
    if (handle_) {
      handle_.destroy();
    }
  }

  bool await_ready() const noexcept { return false; }

  Handle await_suspend(std::coroutine_handle<> caller) noexcept {
    assert(handle_);
    handle_.promise().set_continuation(caller);
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

}  // namespace detail

template <Returnable T>
class [[nodiscard]] Task {
public:
  using promise_type = detail::TaskPromise<T>;
  using Handle = std::coroutine_handle<promise_type>;

  Task() noexcept = default;
  explicit Task(Handle handle) noexcept : handle_(handle) {}

  VEXO_DELETE_COPY(Task);
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

  // Single-consumer: only an rvalue may be awaited, and the awaiter takes the
  // frame. await_suspend symmetrically transfers into the child.
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
}  // namespace vexo::coro
