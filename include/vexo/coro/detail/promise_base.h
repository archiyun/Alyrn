// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: the backend-neutral coroutine contract every Task
// promise shares -- lazy start, symmetric-transfer completion, fatal-on-throw.
// No scheduling, IO, join machinery, value storage, or allocator lives here.
#pragma once

#include <coroutine>
#include <exception>

namespace vexo::coro::detail {

class PromiseBase {
public:
  // Lazy: the body does not run until a consumer resumes the frame.
  std::suspend_always initial_suspend() const noexcept { return {}; }

  // On completion, tail-call into the awaiting coroutine. When nothing awaits,
  // continuation_ stays std::noop_coroutine and resume returns to its caller.
  struct FinalAwaiter {
    std::coroutine_handle<> continuation;

    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept {
      return continuation;
    }
    void await_resume() const noexcept {}
  };
  FinalAwaiter final_suspend() noexcept { return FinalAwaiter{continuation_}; }

  // Exceptions are banned project-wide; reaching here is an unrecoverable bug.
  void unhandled_exception() noexcept { std::terminate(); }

  void set_continuation(std::coroutine_handle<> continuation) noexcept {
    continuation_ = continuation;
  }
  std::coroutine_handle<> continuation() const noexcept { return continuation_; }

protected:
  PromiseBase() = default;
  ~PromiseBase() = default;

private:
  std::coroutine_handle<> continuation_{std::noop_coroutine()};
};

}  // namespace vexo::coro::detail
