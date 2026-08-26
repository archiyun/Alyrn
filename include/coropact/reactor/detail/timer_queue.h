// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <utility>

#include "coropact/memory/object_pool.h"
#include "coropact/reactor/detail/channel.h"
#include "coropact/time/clock.h"
#include "coropact/time/timer.h"
#include "coropact/time/timer_id.h"
#include "coropact/time/timer_index.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

class Loop;

namespace detail {

using ActiveTimerTable = std::unordered_map<std::int64_t, time::Timer*>;

// TimerQueue manages timerfd-driven timer scheduling for one Loop.
//
// TimerQueue owns Timer objects. TimerIndex only indexes them by
// expiration time using the intrusive hook selected at construction.
class TimerQueue {
public:
  COROPACT_DELETE_COPY_MOVE(TimerQueue);

  using TimerCallback = std::function<void()>;

  explicit TimerQueue(Loop* loop, time::TimerIndexKind index = time::TimerIndexKind::kRbTree);
  ~TimerQueue();

  using TimePoint = time::Deadline;
  using Duration = time::Duration;

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
  time::TimerIndex timers_;
  memory::ObjectPool<time::Timer, kTimerQueueMax> timer_pool_;
  ActiveTimerTable active_timers_;
  time::Timer* processing_timer_{nullptr};
  bool processing_timer_cancelled_{false};
};

}  // namespace detail
}  // namespace coropact::reactor
