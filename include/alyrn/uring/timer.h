// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>

#include "alyrn/coro/work.h"
#include "alyrn/detail/loop_shutdown.h"
#include "alyrn/detail/macros.h"
#include "alyrn/result.h"
#include "alyrn/time/clock.h"
#include "alyrn/time/timer_id.h"
#include "alyrn/uring/loop.h"

namespace alyrn::uring {

// Await on the owning Loop thread; positive waits require its active scheduler.
// Returns success after the delay (immediately for nonpositive delays), or
// operation_canceled if the Loop is stopping/stopped or stops before expiry.
// Once expiry fixes success, a later stop does not change that result.
// The Loop must outlive the awaiting coroutine. Destroying that coroutine
// before expiry on the owner thread removes the timer. Once a continuation is
// queued, its frame must remain alive until resumed; destroying it early is a
// checked contract violation.
class [[nodiscard]] SleepAwaiter final {
public:
  SleepAwaiter(Loop& loop, time::Duration delay) noexcept : loop_(&loop), delay_(delay) {}
  ~SleepAwaiter();

  ALYRN_DELETE_COPY_MOVE(SleepAwaiter);

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<void> await_resume() noexcept;

private:
  static void DispatchLoopStop(void* context) noexcept;

  bool Stopping() const noexcept;
  void Complete(Result<void> result) noexcept;
  void ReleaseTimer() noexcept;

  Loop* loop_;
  time::Duration delay_;
  Result<void> result_{};
  coro::ResumeWork resume_work_{};
  time::TimerId timer_{};
  ::alyrn::detail::LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
  bool continuation_pending_{false};
};

inline SleepAwaiter SleepFor(Loop& loop, time::Duration delay) noexcept {
  return SleepAwaiter{loop, delay};
}

}  // namespace alyrn::uring
