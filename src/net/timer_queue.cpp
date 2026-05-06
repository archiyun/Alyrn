#include "runtime/time/timestamp.h"
#include "runtime/net/channel.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/timer_queue.h"
#include "runtime/net/timer.h"
#include "runtime/net/timer_id.h"

#include <sys/timerfd.h>
#include <unistd.h>
#include <cstring>
#include <cassert>

namespace runtime::net {

static int CreateTimerfd() {
  int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  assert(fd >= 0 && "timerfd_create failed");
  return fd;
}

static void SetTimerfd(int timerfd, runtime::time::Timestamp expiration) {
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

  timerfd_channel_->SetReadCallback(
      [this](runtime::time::Timestamp) { HandleRead(); });
  timerfd_channel_->EnableReading();
}

TimerQueue::~TimerQueue() {
  timerfd_channel_->DisableAll();
  timerfd_channel_->Remove();
  ::close(timerfd_);
  for (auto& [seq, timer] : active_timers_) {
    timers_.Erase(timer);
    Reclaim(timer);
  }
  active_timers_.clear();
}

void TimerQueue::Reclaim(Timer* timer) {
  if (timer_pool_.owns(timer)) {
    timer_pool_.Release(timer);
  } else {
    delete timer;
  }
}

TimerId TimerQueue::AddTimer(TimerCallback cb,
                             runtime::time::Timestamp when,
                             double interval) {
  Timer* t = timer_pool_.Acquire(std::move(cb), when, interval);
  assert(t != nullptr && "TimerQueue pool exhausted");
  loop_->RunInLoop([this, t] {
    bool earliest_changed =
        timers_.Empty() || t->Expiration() < timers_.Earliest()->Expiration();
    timers_.Insert(t);
    active_timers_[t->Sequence()] = t;
    if (earliest_changed) {
      ResetTimerfd(t->Expiration());
    }
  });
  return {t, t->Sequence()};
}

void TimerQueue::Cancel(TimerId id) {
  loop_->RunInLoop([this, seq = id.sequence] {
    auto it = active_timers_.find(seq);
    if (it != active_timers_.end()) {
      timers_.Erase(it->second);
      Reclaim(it->second);
      active_timers_.erase(it);
    }
  });
}

void TimerQueue::HandleRead() {
  runtime::time::Timestamp now = runtime::time::Timestamp::Now();
  ReadTimerfd(timerfd_);

  auto expired = GetExpired(now);
  for (Timer* timer : expired) {
    timer->Run();
  }
  Reset(expired, now);
}

std::vector<Timer*> TimerQueue::GetExpired(runtime::time::Timestamp now) {
  return timers_.PopExpired(now);
}

void TimerQueue::Reset(const std::vector<Timer*>& expired, runtime::time::Timestamp now) {
  for (Timer* timer : expired) {
    active_timers_.erase(timer->Sequence());
    if (timer->Repeat()) {
      timer->Restart(now);
      timers_.Insert(timer);
      active_timers_[timer->Sequence()] = timer;
    } else {
      Reclaim(timer);
    }
  }
  if (!timers_.Empty()) {
    ResetTimerfd(timers_.Earliest()->Expiration());
  }
}

void TimerQueue::ResetTimerfd(runtime::time::Timestamp expiration) {
    SetTimerfd(timerfd_, expiration);
}

}  // namespace runtime::net
