// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <functional>

#include "coropact/ds/intrusive_hash_table.h"
#include "coropact/memory/object_pool.h"
#include "coropact/reactor/channel.h"
#include "coropact/time/timer.h"
#include "coropact/time/timer_id.h"
#include "coropact/time/timer_tree.h"
#include "coropact/time/timestamp.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

class EventLoop;

inline constexpr auto kTimerSequenceOf = [](const time::Timer* timer) -> int64_t {
  return timer->sequence();
};
using ActiveTimerTable = ds::IntrusiveHashTable<time::Timer, kTimerSequenceOf>;

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

  time::TimerId AddTimer(TimerCallback callback, time::Timestamp when, double interval_sec);
  void Cancel(time::TimerId id);

private:
  static constexpr std::size_t kTimerQueueMax = 1 << 15;

  void HandleRead();
  static void DispatchRead(void* context) noexcept;
  void ResetTimerfd(time::Timestamp expiration);

  EventLoop* loop_;
  int timerfd_;
  Channel timerfd_channel_;
  time::TimerTree timers_;
  memory::ObjectPool<time::Timer, kTimerQueueMax> timer_pool_;
  ActiveTimerTable active_timers_;
  time::Timer* processing_timer_{nullptr};
  bool processing_timer_cancelled_{false};
};

}  // namespace coropact::reactor
