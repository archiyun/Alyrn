// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/reactor/timer_queue.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include "coropact/base/check.h"
#include "coropact/reactor/event_loop.h"
#include "coropact/time/timer.h"
#include "coropact/time/timer_id.h"
#include "coropact/time/timestamp.h"

namespace coropact::reactor {

static int CreateTimerfd() {
  int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  COROPACT_CHECK(fd >= 0, "TimerQueue: timerfd_create failed");
  return fd;
}

static void SetTimerfd(int timerfd, coropact::time::Timestamp expiration) {
  itimerspec new_value{};
  int64_t us = static_cast<int64_t>(TimeDifference(expiration, time::Timestamp::Now()) * 1e6);
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
    time::Timer* timer = timers_.Earliest();
    active_timers_.Erase(timer);
    timers_.Erase(timer);
    timer_pool_.Release(timer);
  }
}

time::TimerId TimerQueue::AddTimer(TimerCallback cb, time::Timestamp when, double interval) {
  time::Timer* t = timer_pool_.Acquire(std::move(cb), when, interval);
  loop_->RunInLoop([this, t] {
    bool earliest_changed = timers_.Empty() || t->expiration() < timers_.Earliest()->expiration();
    timers_.Insert(t);
    active_timers_.Insert(t);
    if (earliest_changed) {
      ResetTimerfd(t->expiration());
    }
  });
  return {t->sequence()};
}

void TimerQueue::Cancel(coropact::time::TimerId id) {
  loop_->RunInLoop([this, seq = id.sequence] {
    time::Timer* active_timer = active_timers_.Find(seq);
    if (active_timer != nullptr) {
      active_timers_.Erase(active_timer);
      timers_.Erase(active_timer);
      timer_pool_.Release(active_timer);
      return;
    }

    // Missed the registry: the target is mid-callback (its sequence was already
    // erased when it fired), so flag the in-flight timer instead of touching it.
    if (processing_timer_ != nullptr && processing_timer_->sequence() == seq) {
      processing_timer_cancelled_ = true;
    }
  });
}

void TimerQueue::DispatchRead(void* context) noexcept {
  static_cast<TimerQueue*>(context)->HandleRead();
}

void TimerQueue::HandleRead() {
  time::Timestamp now = time::Timestamp::Now();
  ReadTimerfd(timerfd_);

  timers_.PopWhile([now](const coropact::time::Timer* timer) { return timer->expiration() <= now; },
                   [this, now](coropact::time::Timer* timer) {
                     active_timers_.Erase(timer);
                     processing_timer_ = timer;
                     processing_timer_cancelled_ = false;
                     timer->Run();

                     const bool cancelled = processing_timer_cancelled_;
                     processing_timer_ = nullptr;
                     processing_timer_cancelled_ = false;

                     if (timer->repeat() && !cancelled) {
                       timer->Restart(now);
                       timers_.Insert(timer);
                       active_timers_.Insert(timer);
                     } else {
                       timer_pool_.Release(timer);
                     }
                   });

  if (!timers_.Empty()) {
    ResetTimerfd(timers_.Earliest()->expiration());
  }
}

void TimerQueue::ResetTimerfd(time::Timestamp expiration) { SetTimerfd(timerfd_, expiration); }

}  // namespace coropact::reactor
