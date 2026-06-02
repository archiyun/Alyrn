// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "runtime/base/noncopyable.h"
#include "runtime/memory/object_pool.h"
#include "runtime/time/timer.h"
#include "runtime/time/timer_id.h"
#include "runtime/time/timer_tree.h"
#include "runtime/time/timestamp.h"

namespace runtime::net {

class Channel;
class EventLoop;

// TimerQueue manages timerfd-driven timer scheduling for one EventLoop.
//
// TimerQueue owns Timer objects. TimerTree only indexes them by expiration
// time using intrusive red-black tree nodes embedded inside Timer.
class TimerQueue : public runtime::base::NonCopyable {
public:
  using TimerCallback = std::function<void()>;

  explicit TimerQueue(EventLoop* loop);
  ~TimerQueue();

  runtime::time::TimerId AddTimer(TimerCallback cb,
                                  runtime::time::Timestamp when,
                                  double interval_sec);
  void Cancel(runtime::time::TimerId id);

private:
  static constexpr std::size_t kTimerQueueMax = 1 << 15;

  void HandleRead();
  void ResetTimerfd(runtime::time::Timestamp expiration);
  std::vector<runtime::time::Timer*> GetExpired(runtime::time::Timestamp now);
  void Reset(const std::vector<runtime::time::Timer*>& expired, runtime::time::Timestamp now);

  EventLoop* loop_;
  int timerfd_;
  std::unique_ptr<Channel> timerfd_channel_;
  runtime::time::TimerTree timers_;
  runtime::memory::ObjectPool<runtime::time::Timer, kTimerQueueMax> timer_pool_;
  std::unordered_map<int64_t, runtime::time::Timer*> active_timers_;
};

}  // namespace runtime::net
