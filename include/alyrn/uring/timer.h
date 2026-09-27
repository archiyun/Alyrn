// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <functional>
#include <utility>

#include "alyrn/coro/work.h"
#include "alyrn/detail/check.h"
#include "alyrn/detail/loop_shutdown.h"
#include "alyrn/detail/macros.h"
#include "alyrn/detail/timer_channel.h"
#include "alyrn/result.h"
#include "alyrn/time/clock.h"
#include "alyrn/time/timer_id.h"
#include "alyrn/uring/detail/loop_access.h"
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

namespace detail {

struct TimerChannelOps {
  static Result<time::TimerId> Arm(Loop& loop, time::Deadline when, std::function<void()> fire) {
    return loop.RunAt(when, std::move(fire));
  }
  static void Disarm(Loop& loop, time::TimerId id) noexcept { (void)loop.Cancel(id); }
  static void Register(Loop& loop, ::alyrn::detail::LoopShutdownParticipant& participant) noexcept {
    LoopAccess::RegisterShutdownParticipant(loop, participant);
  }
  static void Unregister(Loop& loop,
                         ::alyrn::detail::LoopShutdownParticipant& participant) noexcept {
    LoopAccess::UnregisterShutdownParticipant(loop, participant);
  }
};

}  // namespace detail

// A one-shot timer whose firing is a Channel receive, so it can be selected
// against other channels:
//
//   uring::Timer timeout(loop, 50ms);
//   std::optional<time::Deadline> fired;
//   co_await Select(events >> event, timeout >> fired);
//
// It delivers the time it fired, once. Stop() cancels it and Reset(delay)
// re-arms it; both discard a firing that was not received. When the Loop
// stops, or the ring cannot arm the timer, it closes its channel, so a waiting
// receive completes with an empty optional. Create, use, and destroy it on
// the Loop's owner thread after Init().
class Timer final : public ::alyrn::detail::BasicTimerChannel<Loop, detail::TimerChannelOps> {
public:
  Timer(Loop& loop, time::Duration delay)
      : BasicTimerChannel(loop, delay, time::Duration::zero()) {}

  // Returns whether the timer had not fired yet.
  bool Stop() noexcept { return StopFiring(); }
  void Reset(time::Duration delay) { Rearm(delay, time::Duration::zero()); }
};

// Delivers the time it fired every period, like Timer otherwise. It keeps at
// most one firing for a receiver that falls behind and drops the rest, so a
// slow receiver sees the latest tick instead of a backlog. Stop() pauses it
// and Reset(period) restarts it with a new period.
class Ticker final : public ::alyrn::detail::BasicTimerChannel<Loop, detail::TimerChannelOps> {
public:
  Ticker(Loop& loop, time::Duration period) : BasicTimerChannel(loop, Checked(period), period) {}

  void Stop() noexcept { (void)StopFiring(); }
  void Reset(time::Duration period) { Rearm(Checked(period), period); }

private:
  static time::Duration Checked(time::Duration period) noexcept {
    ALYRN_CHECK(period > time::Duration::zero(), "Ticker period must be positive");
    return period;
  }
};

}  // namespace alyrn::uring
