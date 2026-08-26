// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <utility>

#include "coropact/ds/intrusive_rbtree.h"
#include "coropact/time/clock.h"
#include "coropact/utils/macros.h"

namespace coropact::time {

// Timer is a TimerQueue node, not an application type. Applications schedule
// work with Loop::RunAfter / CancelTimer and cancel with TimerId.
//
// It stores the callback, next expiration time, repeat interval, and the
// intrusive hooks used by TimerIndex. It has no Loop or fd dependency; the
// net layer decides how expirations are delivered.
//
// Both index hooks are base-hooks: TimerIndex recovers the Timer with
// static_cast, so no per-node owner pointer is stored. A Timer is linked into
// at most one timer index at a time. See coropact/time/timer_index.h.
class Timer : public ds::RBTNode<Timer> {
public:
  using TimerCallback = std::function<void()>;

  Timer(TimerCallback callback, Deadline when, Duration interval)
      : timer_callback_(std::move(callback)),
        expiration_(when),
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
  Deadline expiration() const {
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

  void Restart(Deadline now) { expiration_ = now + interval_; }

private:
  TimerCallback timer_callback_;
  Deadline expiration_;
  Duration interval_;
  bool repeat_;
  int64_t sequence_;

  inline static std::atomic<int64_t> next_sequence_;
};

}  // namespace coropact::time
