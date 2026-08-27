// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>

#include "alyrn/base/check.h"
#include "alyrn/result.h"
#include "alyrn/coro/work.h"
#include "alyrn/luring/detail/loop_access.h"
#include "alyrn/luring/detail/result_state.h"
#include "alyrn/luring/loop.h"
#include "alyrn/time/clock.h"

namespace alyrn::luring {

// Suspends the current coroutine and resumes it on the owning Loop.
// Cancellation of the enclosing coroutine is an owner-side protocol; while
// suspended, the awaiter must remain alive just like other Alyrn awaiters.
class SleepAwaiter final {
public:
  SleepAwaiter(Loop& loop, time::Duration delay) noexcept : loop_(&loop), delay_(delay) {}

  bool await_ready() const noexcept { return delay_ <= time::Duration::zero(); }

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
  ALYRN_CHECK(loop_ != nullptr, "LUring sleep operation has no owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(),
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

}  // namespace alyrn::luring
