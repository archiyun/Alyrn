// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <functional>
#include <unordered_map>

#include "alyrn/detail/memory/object_pool.h"
#include "alyrn/time/clock.h"
#include "alyrn/detail/time/timer.h"
#include "alyrn/time/timer_id.h"
#include "alyrn/detail/time/timer_index.h"
#include "alyrn/detail/utils/macros.h"

namespace alyrn::kqueue::detail {

class Poller;

using ActiveTimerTable =
    std::unordered_map<std::int64_t, ::alyrn::detail::time::Timer*>;

/*
 * User-space timer heap for one Loop, woken by a single EVFILT_TIMER.
 *
 * SleepFor and RunAfter are logical timers in this index. The kernel holds
 * one one-shot alarm for the earliest deadline; firing it drains every timer
 * that is due, then re-arms for whatever remains. Per-operation kernel timers
 * are intentionally not used.
 */
class TimerQueue {
public:
  ALYRN_DELETE_COPY_MOVE(TimerQueue);

  using TimerCallback = std::function<void()>;
  using TimePoint = time::Deadline;
  using Duration = time::Duration;

  explicit TimerQueue(
      Poller& poller,
      ::alyrn::detail::time::TimerIndexKind index =
          ::alyrn::detail::time::TimerIndexKind::kRbTree);
  ~TimerQueue();

  time::TimerId AddTimer(TimerCallback callback, TimePoint when, Duration interval);
  void Cancel(time::TimerId id);
  void HandleExpire();

private:
  static constexpr std::size_t kTimerQueueMax = 1 << 15;

  static void DispatchExpire(void* context) noexcept;
  void ArmKernel(TimePoint expiration);

  Poller* poller_;
  ::alyrn::detail::time::TimerIndex timers_;
  ::alyrn::detail::memory::ObjectPool<::alyrn::detail::time::Timer, kTimerQueueMax>
      timer_pool_;
  ActiveTimerTable active_timers_;
  ::alyrn::detail::time::Timer* processing_timer_{nullptr};
  bool processing_timer_cancelled_{false};
};

}  // namespace alyrn::kqueue::detail
