// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <functional>
#include <unordered_map>

#include "coropact/memory/object_pool.h"
#include "coropact/time/clock.h"
#include "coropact/time/timer.h"
#include "coropact/time/timer_id.h"
#include "coropact/time/timer_index.h"
#include "coropact/utils/macros.h"

namespace coropact::kqueue::detail {

class Poller;

using ActiveTimerTable = std::unordered_map<std::int64_t, time::Timer*>;

/*
 * User-space timer heap for one Loop, woken by a single EVFILT_TIMER.
 *
 * SleepFor and ReadSomeFor are logical timers in this index. The kernel holds
 * one one-shot alarm for the earliest deadline; firing it drains every timer
 * that is due, then re-arms for whatever remains. Per-operation kernel timers
 * are intentionally not used.
 */
class TimerQueue {
public:
  COROPACT_DELETE_COPY_MOVE(TimerQueue);

  using TimerCallback = std::function<void()>;
  using TimePoint = time::Deadline;
  using Duration = time::Duration;

  explicit TimerQueue(Poller& poller,
                      time::TimerIndexKind index = time::TimerIndexKind::kRbTree);
  ~TimerQueue();

  time::TimerId AddTimer(TimerCallback callback, TimePoint when, Duration interval);
  void Cancel(time::TimerId id);
  void HandleExpire();

private:
  static constexpr std::size_t kTimerQueueMax = 1 << 15;

  static void DispatchExpire(void* context) noexcept;
  void ArmKernel(TimePoint expiration);

  Poller* poller_;
  time::TimerIndex timers_;
  memory::ObjectPool<time::Timer, kTimerQueueMax> timer_pool_;
  ActiveTimerTable active_timers_;
  time::Timer* processing_timer_{nullptr};
  bool processing_timer_cancelled_{false};
};

}  // namespace coropact::kqueue::detail
