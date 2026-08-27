// SPDX-License-Identifier: MIT
#include "coropact/reactor/detail/timer_queue.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>

#include "coropact/base/check.h"
#include "coropact/reactor/loop.h"
#include "coropact/time/timer_id.h"

namespace coropact::reactor::detail {

static int CreateTimerfd() {
  int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  COROPACT_CHECK(fd >= 0, "TimerQueue: timerfd_create failed");
  return fd;
}

static void SetTimerfd(int timerfd, time::Deadline expiration) {
  itimerspec new_value{};
  int64_t us =
      std::chrono::duration_cast<std::chrono::microseconds>(expiration - time::SteadyNow()).count();
  if (us < 100) {
    us = 100;
  }

  new_value.it_value.tv_sec = us / 1'000'000;
  new_value.it_value.tv_nsec = (us % 1'000'000) * 1000;
  for (;;) {
    if (::timerfd_settime(timerfd, 0, &new_value, nullptr) == 0) {
      return;
    }
    if (errno == EINTR) {
      continue;
    }
    COROPACT_CHECK(false, "TimerQueue: timerfd_settime failed");
  }
}

static void ReadTimerfd(int timerfd) {
  std::uint64_t expirations = 0;
  for (;;) {
    const ssize_t read = ::read(timerfd, &expirations, sizeof(expirations));
    if (read == static_cast<ssize_t>(sizeof(expirations))) {
      return;
    }
    if (read < 0 && errno == EINTR) {
      continue;
    }
    if (read < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return;
    }
    COROPACT_CHECK(false, "TimerQueue: timerfd read failed");
  }
}

TimerQueue::TimerQueue(Loop* loop, time::TimerIndexKind index)
    : timerfd_(CreateTimerfd()), timerfd_channel_(loop, timerfd_), timers_(index) {
  timerfd_channel_.SetReadCallback(&TimerQueue::DispatchRead, this);
  timerfd_channel_.EnableReading();
}

TimerQueue::~TimerQueue() {
  timerfd_channel_.DisableAll();
  timerfd_channel_.Remove();
  ::close(timerfd_);
  while (!timers_.Empty()) {
    time::Timer* timer = timers_.Earliest();
    COROPACT_CHECK(active_timers_.erase(timer->sequence()) == 1,
                   "TimerQueue: destroyed timer is missing from active set");
    COROPACT_CHECK(timers_.Erase(timer), "TimerQueue: destroyed timer is missing from timer tree");
    timer_pool_.Release(timer);
  }
}

time::TimerId TimerQueue::AddTimer(TimerCallback cb, TimePoint when, Duration interval) {
  time::Timer* t = timer_pool_.Acquire(std::move(cb), when, interval);
  bool earliest_changed = timers_.Empty() || t->expiration() < timers_.Earliest()->expiration();
  COROPACT_CHECK(timers_.Insert(t), "TimerQueue: duplicate timer-tree entry");
  COROPACT_CHECK(active_timers_.emplace(t->sequence(), t).second,
                 "TimerQueue: duplicate active timer sequence");
  if (earliest_changed) {
    ResetTimerfd(t->expiration());
  }
  return {t->sequence()};
}

void TimerQueue::Cancel(coropact::time::TimerId id) {
  auto active_it = active_timers_.find(id.sequence);
  if (active_it != active_timers_.end()) {
    time::Timer* active_timer = active_it->second;
    const bool earliest_removed = active_timer == timers_.Earliest();
    active_timers_.erase(active_it);
    COROPACT_CHECK(timers_.Erase(active_timer),
                   "TimerQueue: active timer is missing from timer tree");
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
  const auto now = time::SteadyNow();
  ReadTimerfd(timerfd_);

  timers_.PopWhile([now](const time::Timer* timer) { return timer->expiration() <= now; },
                   [this, now](time::Timer* timer) {
                     COROPACT_CHECK(active_timers_.erase(timer->sequence()) == 1,
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
                     COROPACT_CHECK(active_timers_.emplace(timer->sequence(), timer).second,
                                      "TimerQueue: repeating timer sequence was reused");
                     } else {
                       timer_pool_.Release(timer);
                     }
                   });

  if (!timers_.Empty()) {
    ResetTimerfd(timers_.Earliest()->expiration());
  }
}

void TimerQueue::ResetTimerfd(time::Deadline expiration) { SetTimerfd(timerfd_, expiration); }

}  // namespace coropact::reactor::detail
