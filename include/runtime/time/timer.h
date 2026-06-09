// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <utility>

#include "runtime/base/noncopyable.h"
#include "runtime/ds/intrusive_rbtree.h"
#include "runtime/time/timestamp.h"

namespace runtime::time {

// Timer represents one scheduled callback.
//
// It stores the callback, next expiration time, repeat interval, and an
// intrusive red-black tree hook used by TimerTree. It has no EventLoop or fd
// dependency; the net layer decides how expirations are delivered.
//
// The tree linkage is inherited (base-hook): TimerTree recovers the Timer from
// the hook with static_cast, so no per-node owner pointer is stored. See
// runtime/ds/intrusive_rbtree.h.
class Timer : public runtime::base::NonCopyable,
              public runtime::ds::RBTNode<Timer> {
public:
  using TimerCallback = std::function<void()>;

  Timer(TimerCallback cb, Timestamp when, double interval_sec)
      : timer_callback_(std::move(cb)),
        expiration_(when),
        interval_sec_(interval_sec),
        repeat_(interval_sec > 0.0),
        sequence_(next_sequence_.fetch_add(1, std::memory_order_relaxed)) {}

  void Run() const {
    if (timer_callback_) timer_callback_();
  }

  Timestamp expiration() const { return expiration_; }
  bool repeat() const { return repeat_; }
  int64_t sequence() const { return sequence_; }

  void Restart(Timestamp now) { expiration_ = AddTime(now, interval_sec_); }

private:
  TimerCallback timer_callback_;
  Timestamp expiration_;
  double interval_sec_;
  bool repeat_;
  int64_t sequence_;

  inline static std::atomic<int64_t> next_sequence_;
};

}  // namespace runtime::time
