#include "runtime/net/timer_queue.h"
#include "runtime/net/channel.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/timer.h"
#include "runtime/net/timer_id.h"
#include "runtime/time/timestamp.h"

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
  using namespace runtime::time;
  itimerspec new_value{};
  int64_t us =
      static_cast<int64_t>(TimeDifference(expiration, Timestamp::Now()) * 1e6);
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
  for (auto& [ts, timer] : timers_) {
    delete timer;
  }
}

TimerId TimerQueue::AddTimer(Callback cb,
                             runtime::time::Timestamp when,
                             double interval) {
  Timer* t = new Timer(std::move(cb), when, interval);
  loop_->RunInLoop([this, t] {
    bool earliest_changed =
        timers_.empty() || t->Expiration() < timers_.begin()->first;
    timers_.insert({t->Expiration(), t});
    if (earliest_changed) {
      ResetTimerfd(t->Expiration());
    }
  });
  return {t, t->Sequence()};
}

void TimerQueue::Cancel(TimerId id) {
  loop_->RunInLoop([this, id] {
    auto it = timers_.find({id.timer->Expiration(), id.timer});
    if (it != timers_.end() && it->second->Sequence() == id.sequence) {
      delete it->second;
      timers_.erase(it);
    }
  });
}

void TimerQueue::HandleRead() {
  runtime::time::Timestamp now = runtime::time::Timestamp::Now();
  ReadTimerfd(timerfd_);

  auto expired = GetExpired(now);
  for (auto& [ts, timer] : expired) {
    timer->Run();
  }
  Reset(expired, now);
}

std::vector<TimerQueue::Entry> TimerQueue::GetExpired(runtime::time::Timestamp now) {
  std::vector<Entry> expired;
  auto end = timers_.lower_bound({now, reinterpret_cast<Timer*>(UINTPTR_MAX)});
  for (auto it = timers_.begin(); it != end; ++it) {
    expired.push_back(*it);
  }
  timers_.erase(timers_.begin(), end);
  return expired;
}

void TimerQueue::Reset(const std::vector<Entry>& expired, runtime::time::Timestamp now) {
  for (auto& [ts, timer] : expired) {
    if (timer->Repeat()) {
      timer->Restart(now);
      timers_.insert({timer->Expiration(), timer});
    } else {
      delete timer;
    }
  }
  if (!timers_.empty()) {
    ResetTimerfd(timers_.begin()->first);
  }
}

void TimerQueue::ResetTimerfd(runtime::time::Timestamp expiration) {
    SetTimerfd(timerfd_, expiration);
}

}  // namespace runtime::net
