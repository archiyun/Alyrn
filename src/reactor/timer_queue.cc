// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/reactor/detail/timer_queue.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <chrono>

#include "coropact/base/check.h"
#include "coropact/reactor/loop.h"
#include "coropact/time/timer_id.h"

namespace coropact::reactor::detail {

static int CreateTimerfd() {
  int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  COROPACT_CHECK(fd >= 0, "TimerQueue: timerfd_create failed");
  return fd;
}

static void SetTimerfd(int timerfd, ReactorTimer::TimePoint expiration) {
  itimerspec new_value{};
  int64_t us = std::chrono::duration_cast<std::chrono::microseconds>(
                   expiration - ReactorTimer::Clock::now())
                   .count();
  if (us < 100) {
    us = 100;
  }

  new_value.it_value.tv_sec = us / 1'000'000;
  new_value.it_value.tv_nsec = (us % 1'000'000) * 1000;
  ::timerfd_settime(timerfd, 0, &new_value, nullptr);
}

static void ReadTimerfd(int timerfd) {
  uint64_t how_many;
  ::read(timerfd, &how_many, sizeof(how_many));
}

TimerQueue::TimerQueue(EventLoop* loop)
    : loop_(loop), timerfd_(CreateTimerfd()), timerfd_channel_(loop, timerfd_) {
  timerfd_channel_.SetReadCallback(&TimerQueue::DispatchRead, this);
  timerfd_channel_.EnableReading();
}

TimerQueue::~TimerQueue() {
  timerfd_channel_.DisableAll();
  timerfd_channel_.Remove();
  ::close(timerfd_);
  while (!timers_.Empty()) {
    ReactorTimer* timer = timers_.Earliest();
    (void)(active_timers_.Erase(timer));
    timers_.Erase(timer);
    timer_pool_.Release(timer);
  }
}

time::TimerId TimerQueue::AddTimer(TimerCallback cb, TimePoint when, Duration interval) {
  ReactorTimer* t = timer_pool_.Acquire(std::move(cb), when, interval);
  bool earliest_changed = timers_.Empty() || t->expiration() < timers_.Earliest()->expiration();
  timers_.Insert(t);
  (void)(active_timers_.Insert(t));
  if (earliest_changed) {
    ResetTimerfd(t->expiration());
  }
  return {t->sequence()};
}

void TimerQueue::Cancel(coropact::time::TimerId id) {
  ReactorTimer* active_timer = active_timers_.Find(id.sequence);
  if (active_timer != nullptr) {
    const bool earliest_removed = active_timer == timers_.Earliest();
    (void)(active_timers_.Erase(active_timer));
    timers_.Erase(active_timer);
    timer_pool_.Release(active_timer);
    if (earliest_removed && !timers_.Empty()) {
      ResetTimerfd(timers_.Earliest()->expiration());
    }
    return;
  }

  // Missed the registry: the target is mid-callback (its sequence was already
  // erased when it fired), so flag the in-flight timer instead of touching it.
  if (processing_timer_ != nullptr && processing_timer_->sequence() == id.sequence) {
    processing_timer_cancelled_ = true;
  }
}

void TimerQueue::DispatchRead(void* context) noexcept {
  static_cast<TimerQueue*>(context)->HandleRead();
}

void TimerQueue::HandleRead() {
  const auto now = ReactorTimer::Clock::now();
  ReadTimerfd(timerfd_);

  timers_.PopWhile([now](const detail::ReactorTimer* timer) {
                     return timer->expiration() <= now;
                   },
                   [this, now](ReactorTimer* timer) {
                     (void)(active_timers_.Erase(timer));
                     processing_timer_ = timer;
                     processing_timer_cancelled_ = false;
                     timer->Run();

                     const bool cancelled = processing_timer_cancelled_;
                     processing_timer_ = nullptr;
                     processing_timer_cancelled_ = false;

                     if (timer->repeat() && !cancelled) {
                       timer->Restart(now);
                       timers_.Insert(timer);
                       (void)(active_timers_.Insert(timer));
                     } else {
                       timer_pool_.Release(timer);
                     }
                   });

  if (!timers_.Empty()) {
    ResetTimerfd(timers_.Earliest()->expiration());
  }
}

void TimerQueue::ResetTimerfd(TimePoint expiration) { SetTimerfd(timerfd_, expiration); }

}  // namespace coropact::reactor::detail
