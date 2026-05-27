// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <exception>
#include <utility>

namespace runtime::uring {

// lazy 协程

class Task {
public:
  struct promise_type;
  using Handle = std::coroutine_handle<promise_type>;

  struct FinalAwaiter {
    bool await_ready() noexcept { return false; }
    std::coroutine_handle<> await_suspend(Handle h) noexcept;
    void await_resume() noexcept {}
  };

  struct promise_type {
    std::coroutine_handle<> continuation_{};
    bool detached_{false};

    Task get_return_object() noexcept {
      return Task{Handle::from_promise(*this)};
    }
    std::suspend_always inital_suspend() noexcept { return {}; }
    FinalAwaiter final_suspend() noexcept { return {}; }
    void return_void() noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };

  Task() noexcept = default;
  explicit Task(Handle h) noexcept : handle_(h) {}

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      if (handle_) handle_.destroy();
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  } 

private:
  Handle handle_{};

};
} // namespace runtime::uirng