// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <coroutine>
#include <expected>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/coro/work.h"
#include "vexo/luring/loop.h"

namespace vexo::luring {

// Suspends the current coroutine and resumes it on the owning LUringLoop.
// Cancellation of the enclosing coroutine is an owner-side protocol; while
// suspended, the awaiter must remain alive just like other Vexo awaiters.
class SleepAwaiter final {
public:
  SleepAwaiter(LUringLoop& loop, std::chrono::steady_clock::duration delay) noexcept
      : loop_(&loop), delay_(delay) {}

  [[nodiscard]] bool await_ready() const noexcept {
    return delay_ <= std::chrono::steady_clock::duration::zero();
  }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  base::Result<void> await_resume() noexcept { return std::move(error_); }

private:
  LUringLoop* loop_;
  std::chrono::steady_clock::duration delay_;
  base::Result<void> error_{};
  coro::ResumeWork resume_work_{};
};

inline bool SleepAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  resume_work_.handle = continuation;
  auto timer = loop_->RunAfter(delay_, [this]() noexcept { loop_->Schedule(&resume_work_); });
  if (!timer.has_value()) {
    error_ = std::unexpected(timer.error());
    return false;
  }
  return true;
}

inline SleepAwaiter SleepFor(LUringLoop& loop, std::chrono::steady_clock::duration delay) noexcept {
  return SleepAwaiter(loop, delay);
}

}  // namespace vexo::luring
