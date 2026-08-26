// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <functional>
#include <utility>

#include "coropact/ds/intrusive_hash_table.h"
#include "coropact/ds/intrusive_rbtree.h"
#include "coropact/memory/object_pool.h"
#include "coropact/reactor/detail/channel.h"
#include "coropact/time/clock.h"
#include "coropact/time/timer_id.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

class Loop;

namespace detail {

class Timer final : public ds::RBTNode<Timer>, public ds::HashNode<Timer> {
public:
  using Clock = time::Clock;
  using TimePoint = time::Deadline;
  using Duration = time::Duration;
  using TimerCallback = std::function<void()>;

  Timer(TimerCallback callback, TimePoint expiration, Duration interval)
      : timer_callback_(std::move(callback)),
        expiration_(expiration),
        interval_(interval),
        repeat_(interval > Duration::zero()),
        sequence_(next_sequence_.fetch_add(1, std::memory_order_relaxed)) {}

  COROPACT_DELETE_COPY_MOVE(Timer);

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

inline bool TimerLess(const Timer* lhs, const Timer* rhs) {
  if (lhs->expiration() < rhs->expiration()) {
    return true;
  }
  if (lhs->expiration() > rhs->expiration()) {
    return false;
  }
  return lhs->sequence() < rhs->sequence();
}

inline constexpr auto kTimerSequenceOf = [](const Timer* timer) -> int64_t {
  return timer->sequence();
};
using ActiveTimerTable = ds::IntrusiveHashTable<Timer, kTimerSequenceOf>;
using TimerTree = ds::IntrusiveRBTree<Timer, TimerLess>;

// TimerQueue manages timerfd-driven timer scheduling for one Loop.
//
// TimerQueue owns Timer objects. TimerTree only indexes them by
// expiration time using intrusive red-black tree nodes embedded inside each
// timer.
class TimerQueue {
public:
  COROPACT_DELETE_COPY_MOVE(TimerQueue);

  using TimerCallback = std::function<void()>;

  explicit TimerQueue(Loop* loop);
  ~TimerQueue();

  using TimePoint = Timer::TimePoint;
  using Duration = Timer::Duration;

  time::TimerId AddTimer(TimerCallback callback, TimePoint when, Duration interval);
  void Cancel(time::TimerId id);

private:
  static constexpr std::size_t kTimerQueueMax = 1 << 15;

  void HandleRead();
  static void DispatchRead(void* context) noexcept;
  void ResetTimerfd(TimePoint expiration);

  // The owning loop is reachable through timerfd_channel_.OwnerLoop(); it is
  // deliberately not duplicated here.
  int timerfd_;
  Channel timerfd_channel_;
  TimerTree timers_;
  memory::ObjectPool<Timer, kTimerQueueMax> timer_pool_;
  ActiveTimerTable active_timers_;
  Timer* processing_timer_{nullptr};
  bool processing_timer_cancelled_{false};
};

}  // namespace detail
}  // namespace coropact::reactor
