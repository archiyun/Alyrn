// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>

#include "alyrn/coro/work.h"
#include "alyrn/detail/check.h"
#include "alyrn/result.h"
#include "alyrn/time/clock.h"
#include "alyrn/uring/detail/loop_access.h"
#include "alyrn/uring/detail/result_state.h"
#include "alyrn/uring/loop.h"

namespace alyrn::uring {

// Suspends the current coroutine and resumes it on the owning Loop.
// Cancellation of the enclosing coroutine is an owner-side protocol; while
// suspended, the awaiter must remain alive just like other Alyrn awaiters.
class [[nodiscard]] SleepAwaiter final {
public:
  SleepAwaiter(Loop& loop, time::Duration delay) noexcept : loop_(&loop), delay_(delay) {}

  bool await_ready() const noexcept { return delay_ <= time::Duration::zero(); }
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<void> await_resume() noexcept {
    return result_.IsImmediate() ? result_.Take() : Result<void>{};
  }

private:
  Loop* loop_;
  time::Duration delay_;
  detail::ResultState<void> result_;
  coro::ResumeWork resume_work_{};
};

inline bool SleepAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Uring sleep operation has no owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Uring sleep operation called from wrong Loop thread");
  resume_work_.SetHandle(continuation);
  auto timer = loop_->RunAfter(
      delay_, [this]() noexcept { detail::LoopAccess::ScheduleCompletion(*loop_, &resume_work_); });
  if (!timer.HasValue()) {
    result_.SetError(timer.Error());
    return false;
  }
  return true;
}

inline SleepAwaiter SleepFor(Loop& loop, time::Duration delay) noexcept {
  return SleepAwaiter{loop, delay};
}

}  // namespace alyrn::uring
