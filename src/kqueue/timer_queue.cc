// SPDX-License-Identifier: MIT
#include "coropact/kqueue/detail/timer_queue.h"

#include <chrono>

#include "coropact/base/check.h"
#include "coropact/kqueue/detail/kqueue_poller.h"

namespace coropact::kqueue::detail {

TimerQueue::TimerQueue(KqueuePoller& poller) : poller_(&poller) {
  poller_->SetTimerExpireHandler(&TimerQueue::DispatchExpire, this);
}

TimerQueue::~TimerQueue() {
  poller_->SetTimerExpireHandler(nullptr, nullptr);
  while (!timers_.Empty()) {
    time::Timer* timer = timers_.Earliest();
    COROPACT_CHECK(active_timers_.Erase(timer),
                   "TimerQueue: destroyed timer is missing from active set");
    COROPACT_CHECK(timers_.Erase(timer), "TimerQueue: destroyed timer is missing from timer tree");
    timer_pool_.Release(timer);
  }
  poller_->DisarmTimer();
}

time::TimerId TimerQueue::AddTimer(TimerCallback cb, TimePoint when, Duration interval) {
  time::Timer* timer = timer_pool_.Acquire(std::move(cb), when, interval);
  const bool earliest_changed =
      timers_.Empty() || timer->expiration() < timers_.Earliest()->expiration();
  COROPACT_CHECK(timers_.Insert(timer), "TimerQueue: duplicate timer-tree entry");
  COROPACT_CHECK(active_timers_.Insert(timer), "TimerQueue: duplicate active timer sequence");
  if (earliest_changed) {
    ArmKernel(timer->expiration());
  }
  return {timer->sequence()};
}

void TimerQueue::Cancel(time::TimerId id) {
  time::Timer* active_timer = active_timers_.Find(id.sequence);
  if (active_timer != nullptr) {
    const bool earliest_removed = active_timer == timers_.Earliest();
    COROPACT_CHECK(active_timers_.Erase(active_timer), "TimerQueue: active timer is missing");
    COROPACT_CHECK(timers_.Erase(active_timer),
                   "TimerQueue: active timer is missing from timer tree");
    timer_pool_.Release(active_timer);
    if (timers_.Empty()) {
      poller_->DisarmTimer();
    } else if (earliest_removed) {
      ArmKernel(timers_.Earliest()->expiration());
    }
    return;
  }

  // Missed the registry: the target is mid-callback (its sequence was already
  // erased when it fired), so flag the in-flight timer instead of touching it.
  if (processing_timer_ != nullptr && processing_timer_->sequence() == id.sequence) {
    processing_timer_cancelled_ = true;
  }
}

void TimerQueue::DispatchExpire(void* context) noexcept {
  static_cast<TimerQueue*>(context)->HandleExpire();
}

void TimerQueue::HandleExpire() {
  const auto now = time::SteadyNow();

  timers_.PopWhile([now](const time::Timer* timer) { return timer->expiration() <= now; },
                   [this, now](time::Timer* timer) {
                     COROPACT_CHECK(active_timers_.Erase(timer),
                                    "TimerQueue: expired timer is missing from active set");
                     processing_timer_ = timer;
                     processing_timer_cancelled_ = false;
                     timer->Run();

                     const bool cancelled = processing_timer_cancelled_;
                     processing_timer_ = nullptr;
                     processing_timer_cancelled_ = false;

                     if (timer->repeat() && !cancelled) {
                       timer->Restart(now);
                       COROPACT_CHECK(timers_.Insert(timer),
                                      "TimerQueue: repeating timer is already in timer tree");
                       COROPACT_CHECK(active_timers_.Insert(timer),
                                      "TimerQueue: repeating timer sequence was reused");
                     } else {
                       timer_pool_.Release(timer);
                     }
                   });

  if (!timers_.Empty()) {
    ArmKernel(timers_.Earliest()->expiration());
  }
}

void TimerQueue::ArmKernel(TimePoint expiration) {
  const auto remaining = expiration - time::SteadyNow();
  poller_->ArmOneShotTimer(std::chrono::duration_cast<std::chrono::nanoseconds>(remaining).count());
}

}  // namespace coropact::kqueue::detail
