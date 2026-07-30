// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <coroutine>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/coro/work.h"
#include "coropact/luring/detail/result_state.h"
#include "coropact/luring/loop.h"

namespace coropact::luring {

// Suspends the current coroutine and resumes it on the owning LUringLoop.
// Cancellation of the enclosing coroutine is an owner-side protocol; while
// suspended, the awaiter must remain alive just like other CoroPact awaiters.
class SleepAwaiter final {
public:
  SleepAwaiter(LUringLoop& loop, std::chrono::steady_clock::duration delay) noexcept
      : loop_(&loop), delay_(delay) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return delay_ <= std::chrono::steady_clock::duration::zero();
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  base::Result<void> await_resume() noexcept {
    if (!result_.IsImmediate()) {
      return {};
    }
    return result_.Take();
  }

private:
  LUringLoop* loop_;
  std::chrono::steady_clock::duration delay_;
  detail::LUringResultState<void> result_;
  coro::ResumeWork resume_work_{};
};

inline bool SleepAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  COROPACT_CHECK(loop_ != nullptr, "LUring sleep operation has no owner loop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "LUring sleep operation called from wrong LUringLoop thread");
  resume_work_.SetHandle(continuation);
  auto timer =
      loop_->RunAfter(delay_, [this]() noexcept { loop_->ScheduleCompletion(&resume_work_); });
  if (!timer.has_value()) {
    result_.SetError(timer.error());
    return false;
  }
  return true;
}

inline SleepAwaiter SleepFor(LUringLoop& loop, std::chrono::steady_clock::duration delay) noexcept {
  return SleepAwaiter{loop, delay};
}

}  // namespace coropact::luring
