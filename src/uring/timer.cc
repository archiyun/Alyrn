// SPDX-License-Identifier: MIT
#include "alyrn/uring/timer.h"

#include <cerrno>
#include <expected>
#include <utility>

#include "alyrn/coro/scheduler.h"
#include "alyrn/detail/check.h"
#include "alyrn/uring/detail/loop_access.h"

namespace alyrn::uring {

SleepAwaiter::~SleepAwaiter() {
  ALYRN_CHECK(!continuation_pending_, "SleepFor destroyed with a queued continuation");
  ReleaseTimer();
}

bool SleepAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Uring sleep operation has no owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Uring sleep operation called from wrong Loop thread");
  if (Stopping()) {
    result_ = std::unexpected(Errno(ECANCELED));
    return false;
  }
  if (delay_ <= time::Duration::zero()) {
    return false;
  }
  ALYRN_CHECK(coro::Scheduler::TryCurrent() == loop_,
              "SleepFor requires the owning Loop scheduler");

  auto timer = loop_->RunAfter(delay_, [this] {
    // The queue already removed this timer before running it.
    timer_ = {};
    Complete(Stopping() ? Result<void>(std::unexpected(Errno(ECANCELED))) : Result<void>{});
  });
  if (!timer.HasValue()) {
    result_ = std::unexpected(timer.Error());
    return false;
  }
  timer_ = *timer;
  resume_work_.SetHandle(continuation);
  detail::LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
  return true;
}

Result<void> SleepAwaiter::await_resume() noexcept {
  continuation_pending_ = false;
  return result_;
}

void SleepAwaiter::DispatchLoopStop(void* context) noexcept {
  static_cast<SleepAwaiter*>(context)->Complete(std::unexpected(Errno(ECANCELED)));
}

bool SleepAwaiter::Stopping() const noexcept {
  const backend::LoopState state = loop_->State();
  return state == backend::LoopState::kStopping || state == backend::LoopState::kStopped;
}

void SleepAwaiter::Complete(Result<void> result) noexcept {
  result_ = std::move(result);
  ReleaseTimer();
  continuation_pending_ = true;
  detail::LoopAccess::ScheduleCompletion(*loop_, &resume_work_);
}

void SleepAwaiter::ReleaseTimer() noexcept {
  if (timer_.Valid()) {
    (void)loop_->CancelTimer(std::exchange(timer_, {}));
  }
  if (shutdown_participant_.InList()) {
    detail::LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  }
}

}  // namespace alyrn::uring
