// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <utility>

#include "coropact/ds/intrusive_hash_table.h"
#include "coropact/ds/intrusive_rbtree.h"
#include "coropact/memory/object_pool.h"
#include "coropact/reactor/detail/channel.h"
#include "coropact/time/timer_id.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

class EventLoop;

namespace detail {


class ReactorTimer final : public ds::RBTNode<ReactorTimer>, public ds::HashNode<ReactorTimer> {
public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;
  using Duration = Clock::duration;
  using TimerCallback = std::function<void()>;

  ReactorTimer(TimerCallback callback, TimePoint expiration, Duration interval)
      : timer_callback_(std::move(callback)),
        expiration_(expiration),
        interval_(interval),
        repeat_(interval > Duration::zero()),
        sequence_(next_sequence_.fetch_add(1, std::memory_order_relaxed)) {}

  COROPACT_DELETE_COPY_MOVE(ReactorTimer);

  void Run() const {
    if (timer_callback_) {
      timer_callback_();
    }
  }

  [[nodiscard]]
  TimePoint expiration() const {
    return expiration_;
  }
  [[nodiscard]]
  bool repeat() const {
    return repeat_;
  }
  [[nodiscard]]
  int64_t sequence() const {
    return sequence_;
  }

  void Restart(TimePoint now) { expiration_ = now + interval_; }

private:
  TimerCallback timer_callback_;
  TimePoint expiration_;
  Duration interval_;
  bool repeat_;
  int64_t sequence_;

  inline static std::atomic<int64_t> next_sequence_;
};

inline bool ReactorTimerLess(const ReactorTimer* lhs, const ReactorTimer* rhs) {
  if (lhs->expiration() < rhs->expiration()) {
    return true;
  }
  if (lhs->expiration() > rhs->expiration()) {
    return false;
  }
  return lhs->sequence() < rhs->sequence();
}

inline constexpr auto kTimerSequenceOf = [](const ReactorTimer* timer) -> int64_t {
  return timer->sequence();
};
using ActiveTimerTable = ds::IntrusiveHashTable<ReactorTimer, kTimerSequenceOf>;
using TimerTree = ds::IntrusiveRBTree<ReactorTimer, ReactorTimerLess>;

// TimerQueue manages timerfd-driven timer scheduling for one EventLoop.
//
// TimerQueue owns ReactorTimer objects. TimerTree only indexes them by
// expiration time using intrusive red-black tree nodes embedded inside each
// timer.
class TimerQueue {
public:
  COROPACT_DELETE_COPY_MOVE(TimerQueue);

  using TimerCallback = std::function<void()>;

  explicit TimerQueue(EventLoop* loop);
  ~TimerQueue();

  using TimePoint = ReactorTimer::TimePoint;
  using Duration = ReactorTimer::Duration;

  time::TimerId AddTimer(TimerCallback callback, TimePoint when, Duration interval);
  void Cancel(time::TimerId id);

private:
  static constexpr std::size_t kTimerQueueMax = 1 << 15;

  void HandleRead();
  static void DispatchRead(void* context) noexcept;
  void ResetTimerfd(TimePoint expiration);

  EventLoop* loop_;
  int timerfd_;
  Channel timerfd_channel_;
  TimerTree timers_;
  memory::ObjectPool<ReactorTimer, kTimerQueueMax> timer_pool_;
  ActiveTimerTable active_timers_;
  ReactorTimer* processing_timer_{nullptr};
  bool processing_timer_cancelled_{false};
};

}  // namespace detail
}  // namespace coropact::reactor
