// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <functional>

#include "vexo/ds/intrusive_hash_table.h"
#include "vexo/memory/object_pool.h"
#include "vexo/net/channel.h"
#include "vexo/time/timer.h"
#include "vexo/time/timer_id.h"
#include "vexo/time/timer_tree.h"
#include "vexo/time/timestamp.h"
#include "vexo/utils/macros.h"

namespace vexo::net {

class EventLoop;

inline constexpr auto kTimerSequenceOf = [](const vexo::time::Timer* timer) -> int64_t {
  return timer->sequence();
};
using ActiveTimerTable = vexo::ds::IntrusiveHashTable<vexo::time::Timer, kTimerSequenceOf>;

// TimerQueue manages timerfd-driven timer scheduling for one EventLoop.
//
// TimerQueue owns Timer objects. TimerTree only indexes them by expiration
// time using intrusive red-black tree nodes embedded inside Timer.
class TimerQueue {
public:
  VEXO_DELETE_COPY_MOVE(TimerQueue);

  using TimerCallback = std::function<void()>;

  explicit TimerQueue(EventLoop* loop);
  ~TimerQueue();

  vexo::time::TimerId AddTimer(TimerCallback callback, vexo::time::Timestamp when,
                               double interval_sec);
  void Cancel(vexo::time::TimerId id);

private:
  static constexpr std::size_t kTimerQueueMax = 1 << 15;

  void HandleRead();
  void ResetTimerfd(vexo::time::Timestamp expiration);

  EventLoop* loop_;
  int timerfd_;
  Channel timerfd_channel_;
  vexo::time::TimerTree timers_;
  vexo::memory::ObjectPool<vexo::time::Timer, kTimerQueueMax> timer_pool_;
  ActiveTimerTable active_timers_;
  vexo::time::Timer* processing_timer_{nullptr};
  bool processing_timer_cancelled_{false};
};

}  // namespace vexo::net
