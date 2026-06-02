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
  for (auto& [seq, timer] : active_timers_) {
    timers_.Erase(timer);
    timer_pool_.Release(timer);
  }
  active_timers_.clear();
}

runtime::time::TimerId TimerQueue::AddTimer(TimerCallback cb,
                                            runtime::time::Timestamp when,
                                            double interval) {
  runtime::time::Timer* t = timer_pool_.Acquire(std::move(cb), when, interval);
  loop_->RunInLoop([this, t] {
    bool earliest_changed =
        timers_.empty() || t->expiration() < timers_.earliest()->expiration();
    timers_.Insert(t);
    active_timers_[t->sequence()] = t;
    if (earliest_changed) {
      ResetTimerfd(t->expiration());
    }
  });
  return {t, t->sequence()};
}

void TimerQueue::Cancel(runtime::time::TimerId id) {
  loop_->RunInLoop([this, seq = id.sequence] {
    auto it = active_timers_.find(seq);
    if (it != active_timers_.end()) {
      timers_.Erase(it->second);
      timer_pool_.Release(it->second);
      active_timers_.erase(it);
    }
  });
}

void TimerQueue::HandleRead() {
  runtime::time::Timestamp now = runtime::time::Timestamp::Now();
  ReadTimerfd(timerfd_);

  auto expired = GetExpired(now);
  for (runtime::time::Timer* timer : expired) {
    timer->Run();
  }
  Reset(expired, now);
}

std::vector<runtime::time::Timer*> TimerQueue::GetExpired(runtime::time::Timestamp now) {
  return timers_.PopWhile([now](const runtime::time::Timer* t) {
    return t->expiration() <= now;
  });
}

void TimerQueue::Reset(const std::vector<runtime::time::Timer*>& expired,
                       runtime::time::Timestamp now) {
  for (runtime::time::Timer* timer : expired) {
    active_timers_.erase(timer->sequence());
    if (timer->repeat()) {
      timer->Restart(now);
      timers_.Insert(timer);
      active_timers_[timer->sequence()] = timer;
    } else {
      timer_pool_.Release(timer);
    }
  }
  if (!timers_.empty()) {
    ResetTimerfd(timers_.earliest()->expiration());
  }
}

void TimerQueue::ResetTimerfd(runtime::time::Timestamp expiration) {
    set_timerfd(timerfd_, expiration);
}

}  // namespace runtime::net
