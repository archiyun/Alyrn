// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>

#include "coropact/base/check.h"
#include "coropact/result.h"
#include "coropact/coro/work.h"
#include "coropact/luring/detail/loop_access.h"
#include "coropact/luring/detail/result_state.h"
#include "coropact/luring/loop.h"
#include "coropact/time/clock.h"

namespace coropact::luring {

// Suspends the current coroutine and resumes it on the owning Loop.
// Cancellation of the enclosing coroutine is an owner-side protocol; while
// suspended, the awaiter must remain alive just like other CoroPact awaiters.
class SleepAwaiter final {
public:
  SleepAwaiter(Loop& loop, time::Duration delay) noexcept : loop_(&loop), delay_(delay) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return delay_ <= time::Duration::zero();
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  Result<void> await_resume() noexcept {
    if (!result_.IsImmediate()) {
      return {};
    }
    return result_.Take();
  }

private:
  Loop* loop_;
  time::Duration delay_;
  detail::ResultState<void> result_;
  coro::ResumeWork resume_work_{};
};

inline bool SleepAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  COROPACT_CHECK(loop_ != nullptr, "LUring sleep operation has no owner loop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "LUring sleep operation called from wrong Loop thread");
  resume_work_.SetHandle(continuation);
  auto timer = loop_->RunAfter(
      delay_, [this]() noexcept { detail::LoopAccess::ScheduleCompletion(*loop_, &resume_work_); });
  if (!timer.has_value()) {
    result_.SetError(timer.error());
    return false;
  }
  return true;
}

inline SleepAwaiter SleepFor(Loop& loop, time::Duration delay) noexcept {
  return SleepAwaiter{loop, delay};
}

}  // namespace coropact::luring
