// SPDX-License-Identifier: MIT
#include "alyrn/kqueue/detail/timer_queue.h"

#include <chrono>

#include "alyrn/detail/check.h"
#include "alyrn/kqueue/detail/poller.h"

namespace alyrn::kqueue::detail {

TimerQueue::TimerQueue(Poller& poller, TimerIndexKind index)
    : poller_(&poller), timers_(index) {
  poller_->SetTimerExpireHandler(&TimerQueue::DispatchExpire, this);
}

TimerQueue::~TimerQueue() {
  poller_->SetTimerExpireHandler(nullptr, nullptr);
  while (!timers_.Empty()) {
    auto* timer = timers_.Earliest();
    ALYRN_CHECK(active_timers_.erase(timer->sequence()) == 1,
                   "TimerQueue: destroyed timer is missing from active set");
    ALYRN_CHECK(timers_.Erase(timer), "TimerQueue: destroyed timer is missing from timer tree");
    timer_pool_.Release(timer);
  }
  poller_->DisarmTimer();
}

time::TimerId TimerQueue::AddTimer(TimerCallback cb, TimePoint when, Duration interval) {
  auto* timer = timer_pool_.Acquire(std::move(cb), when, interval);
  const bool earliest_changed =
      timers_.Empty() || timer->expiration() < timers_.Earliest()->expiration();
  ALYRN_CHECK(timers_.Insert(timer), "TimerQueue: duplicate timer-tree entry");
  ALYRN_CHECK(active_timers_.emplace(timer->sequence(), timer).second,
                 "TimerQueue: duplicate active timer sequence");
  if (earliest_changed) {
    ArmKernel(timer->expiration());
  }
  return {timer->sequence()};
}

void TimerQueue::Cancel(time::TimerId id) {
  auto active_it = active_timers_.find(id.sequence);
  if (active_it != active_timers_.end()) {
    auto* active_timer = active_it->second;
    const bool earliest_removed = active_timer == timers_.Earliest();
    active_timers_.erase(active_it);
    ALYRN_CHECK(timers_.Erase(active_timer),
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

  timers_.PopWhile(
      [now](const Timer* timer) { return timer->expiration() <= now; },
      [this, now](Timer* timer) {
                     ALYRN_CHECK(active_timers_.erase(timer->sequence()) == 1,
                                    "TimerQueue: expired timer is missing from active set");
                     processing_timer_ = timer;
                     processing_timer_cancelled_ = false;
                     timer->Run();

                     const bool cancelled = processing_timer_cancelled_;
                     processing_timer_ = nullptr;
                     processing_timer_cancelled_ = false;

                     if (timer->repeat() && !cancelled) {
                       timer->Restart(now);
                       ALYRN_CHECK(timers_.Insert(timer),
                                      "TimerQueue: repeating timer is already in timer tree");
                       ALYRN_CHECK(active_timers_.emplace(timer->sequence(), timer).second,
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

}  // namespace alyrn::kqueue::detail
