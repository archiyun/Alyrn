// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/net/timer_queue.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <cassert>
#include <cstring>

#include "runtime/net/channel.h"
#include "runtime/net/event_loop.h"
#include "runtime/time/timer.h"
#include "runtime/time/timer_id.h"
#include "runtime/time/timestamp.h"

namespace runtime::net {

static int CreateTimerfd() {
  int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  assert(fd >= 0 && "timerfd_create failed");
  return fd;
}

static void set_timerfd(int timerfd, runtime::time::Timestamp expiration) {
  itimerspec new_value{};
  int64_t us =
      static_cast<int64_t>(TimeDifference(expiration, runtime::time::Timestamp::Now()) * 1e6);
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
    : loop_(loop),
      timerfd_(CreateTimerfd()),
      timerfd_channel_(std::make_unique<Channel>(loop, timerfd_)) {

  timerfd_channel_->set_read_callback(
      [this](runtime::time::Timestamp) { HandleRead(); });
  timerfd_channel_->EnableReading();
}

TimerQueue::~TimerQueue() {
  timerfd_channel_->DisableAll();
  timerfd_channel_->Remove();
  ::close(timerfd_);
  while (!timers_.empty()) {
    runtime::time::Timer* timer = timers_.earliest();
    active_timers_.Erase(timer);
    timers_.Erase(timer);
    timer_pool_.Release(timer);
  }
}

runtime::time::TimerId TimerQueue::AddTimer(TimerCallback cb,
                                            runtime::time::Timestamp when,
                                            double interval) {
  runtime::time::Timer* t = timer_pool_.Acquire(std::move(cb), when, interval);
  loop_->RunInLoop([this, t] {
    bool earliest_changed =
        timers_.empty() || t->expiration() < timers_.earliest()->expiration();
    timers_.Insert(t);
    active_timers_.Insert(t);
    if (earliest_changed) {
      ResetTimerfd(t->expiration());
    }
  });
  return {t->sequence()};
}

void TimerQueue::Cancel(runtime::time::TimerId id) {
  loop_->RunInLoop([this, seq = id.sequence] {
    runtime::time::Timer* active_timer = active_timers_.Find(seq);
    if (active_timer != nullptr) {
      active_timers_.Erase(active_timer);
      timers_.Erase(active_timer);
      timer_pool_.Release(active_timer);
      return;
    }

    // Missed the registry: the target is mid-callback (its sequence was already
    // erased when it fired), so flag the in-flight timer instead of touching it.
    if (processing_timer_ != nullptr &&
        processing_timer_->sequence() == seq) {
      processing_timer_cancelled_ = true;
    }
  });
}

void TimerQueue::HandleRead() {
  runtime::time::Timestamp now = runtime::time::Timestamp::Now();
  ReadTimerfd(timerfd_);

  timers_.PopWhile(
      [now](const runtime::time::Timer* timer) {
        return timer->expiration() <= now;
      },
      [this, now](runtime::time::Timer* timer) {
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

  if (!timers_.empty()) {
    ResetTimerfd(timers_.earliest()->expiration());
  }
}

void TimerQueue::ResetTimerfd(runtime::time::Timestamp expiration) {
  set_timerfd(timerfd_, expiration);
}

}  // namespace runtime::net
