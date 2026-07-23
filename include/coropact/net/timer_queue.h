// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <functional>

#include "coropact/ds/intrusive_hash_table.h"
#include "coropact/memory/object_pool.h"
#include "coropact/net/channel.h"
#include "coropact/time/timer.h"
#include "coropact/time/timer_id.h"
#include "coropact/time/timer_tree.h"
#include "coropact/time/timestamp.h"
#include "coropact/utils/macros.h"

namespace coropact::net {

class EventLoop;

inline constexpr auto kTimerSequenceOf = [](const coropact::time::Timer* timer) -> int64_t {
  return timer->sequence();
};
using ActiveTimerTable = coropact::ds::IntrusiveHashTable<coropact::time::Timer, kTimerSequenceOf>;

// TimerQueue manages timerfd-driven timer scheduling for one EventLoop.
//
// TimerQueue owns Timer objects. TimerTree only indexes them by expiration
// time using intrusive red-black tree nodes embedded inside Timer.
class TimerQueue {
public:
  COROPACT_DELETE_COPY_MOVE(TimerQueue);

  using TimerCallback = std::function<void()>;

  explicit TimerQueue(EventLoop* loop);
  ~TimerQueue();

  coropact::time::TimerId AddTimer(TimerCallback callback, coropact::time::Timestamp when,
                               double interval_sec);
  void Cancel(coropact::time::TimerId id);

private:
  static constexpr std::size_t kTimerQueueMax = 1 << 15;

  void HandleRead();
  void ResetTimerfd(coropact::time::Timestamp expiration);

  EventLoop* loop_;
  int timerfd_;
  Channel timerfd_channel_;
  coropact::time::TimerTree timers_;
  coropact::memory::ObjectPool<coropact::time::Timer, kTimerQueueMax> timer_pool_;
  ActiveTimerTable active_timers_;
  coropact::time::Timer* processing_timer_{nullptr};
  bool processing_timer_cancelled_{false};
};

}  // namespace coropact::net
