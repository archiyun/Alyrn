// SPDX-License-Identifier: MIT
#include "alyrn/epoll/timer.h"

#include <cerrno>
#include <coroutine>
#include <expected>
#include <utility>

#include "alyrn/detail/check.h"
#include "alyrn/detail/scheduler_continuation.h"
#include "alyrn/epoll/detail/loop_access.h"
#include "alyrn/epoll/loop.h"

namespace alyrn::epoll {
namespace {

class SleepAwaiter {
public:
  SleepAwaiter(Loop& loop, time::Duration delay) noexcept : loop_(loop), delay_(delay) {}

  ~SleepAwaiter() {
    ALYRN_CHECK(!continuation_pending_, "SleepFor destroyed with a queued continuation");
    ReleaseTimer();
  }

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    ALYRN_CHECK(loop_.IsInLoopThread(), "SleepFor called from wrong Loop thread");
    if (Stopping()) {
      result_ = std::unexpected(Errno(ECANCELED));
      return false;
    }
    if (delay_ <= time::Duration::zero()) return false;
    ALYRN_CHECK(coro::Scheduler::TryCurrent() == &loop_,
                "SleepFor requires the owning Loop scheduler");
    continuation_.Bind(continuation);
    detail::LoopAccess::RegisterShutdownParticipant(loop_, shutdown_participant_);
    timer_ = loop_.RunAfter(delay_, [this] {
      timer_ = {};
      Complete(Stopping() ? Result<void>(std::unexpected(Errno(ECANCELED))) : Result<void>{});
    });
    return true;
  }

  Result<void> await_resume() noexcept {
    continuation_pending_ = false;
    return result_;
  }

private:
  bool Stopping() const noexcept {
    const backend::LoopState state = loop_.State();
    return state == backend::LoopState::kStopping || state == backend::LoopState::kStopped;
  }

  static void DispatchLoopStop(void* context) noexcept {
    static_cast<SleepAwaiter*>(context)->Complete(std::unexpected(Errno(ECANCELED)));
  }

  void Complete(Result<void> result) noexcept {
    result_ = std::move(result);
    ReleaseTimer();
    continuation_pending_ = true;
    continuation_.Schedule();
  }

  void ReleaseTimer() noexcept {
    if (timer_.Valid()) loop_.Cancel(std::exchange(timer_, {}));
    if (shutdown_participant_.InList()) {
      detail::LoopAccess::UnregisterShutdownParticipant(loop_, shutdown_participant_);
    }
  }

  Loop& loop_;
  time::Duration delay_;
  Result<void> result_;
  ::alyrn::detail::SchedulerContinuation continuation_;
  time::TimerId timer_;
  detail::LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
  bool continuation_pending_{false};
};

}  // namespace

Task<Result<void>> SleepFor(Loop& loop, time::Duration delay) {
  co_return co_await SleepAwaiter(loop, delay);
}

}  // namespace alyrn::epoll
