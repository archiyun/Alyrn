// SPDX-License-Identifier: MIT
#pragma once

#include <liburing.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>

#include "coropact/result.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/detail/op_hook.h"
#include "coropact/time/timer.h"
#include "coropact/time/timer_id.h"
#include "coropact/time/timer_index.h"

namespace coropact::luring {

class Loop;

namespace detail {

struct TimerDriverTag;
struct TimerControlTag;

// One timer queue belongs to one Loop and is only accessed by that
// loop's thread. The timer tree stays in user space; one io_uring timeout is
// used to wake the loop for the earliest timer.
class TimerQueue final : public OpHook<TimerQueue, TimerDriverTag>,
                               public OpHook<TimerQueue, TimerControlTag> {
  friend void DispatchTimerDriverComplete(Op* op) noexcept;
  friend void DispatchTimerControlComplete(Op* op) noexcept;

public:
  using DriverOpHook = OpHook<TimerQueue, TimerDriverTag>;
  using ControlOpHook = OpHook<TimerQueue, TimerControlTag>;
  using TimerCallback = std::function<void()>;

  explicit TimerQueue(Loop* loop,
                      time::TimerIndexKind index = time::TimerIndexKind::kRbTree) noexcept
      : DriverOpHook(OpKind::kTimerDriverComplete),
        ControlOpHook(OpKind::kTimerControlComplete),
        loop_(loop),
        timers_(index) {}
  ~TimerQueue() noexcept;

  TimerQueue(const TimerQueue&) = delete;
  TimerQueue& operator=(const TimerQueue&) = delete;

  [[nodiscard]]
  Result<time::TimerId> AddAfter(time::Duration delay, TimerCallback callback);
  [[nodiscard]]
  Result<time::TimerId> AddTimer(TimerCallback callback, time::Deadline deadline);
  Result<void> Cancel(time::TimerId id) noexcept;

  // Drops every logical timer without invoking user callbacks. The loop calls
  // this only after all submitted timer requests have reached a physical
  // terminal CQE. The destructor repeats the operation defensively so the
  // intrusive index never outlives the Timer objects owned by active_.
  void DiscardAll() noexcept;

private:
  static void OnDriverComplete(Op* op) noexcept;
  static void OnControlComplete(Op* op) noexcept;

  void HandleDriverComplete(Op* op) noexcept;
  void HandleControlComplete(Op* op) noexcept;
  void ProcessExpired() noexcept;
  void ReconcileOrStop() noexcept;
  [[nodiscard]] Result<void> Reconcile() noexcept;
  [[nodiscard]] Result<void> Arm(time::Deadline deadline) noexcept;
  [[nodiscard]] Result<void> ArmFallback(time::Deadline deadline) noexcept;
  [[nodiscard]] Result<void> Update(time::Deadline deadline) noexcept;

  Op* DriverOp() noexcept { return DriverOpHook::Operation(); }
  Op* ControlOp() noexcept { return ControlOpHook::Operation(); }

  Loop* loop_;
  time::TimerIndex timers_;
  std::unordered_map<std::int64_t, std::unique_ptr<time::Timer>> active_;

  bool driver_armed_{false};
  bool control_pending_{false};
  bool control_is_fallback_{false};
  bool fallback_armed_{false};
  bool timeout_update_supported_{true};
  time::Deadline driver_deadline_{};
  time::Deadline requested_deadline_{};

  // io_uring_prep_timeout stores a user pointer in the SQE. These objects
  // must therefore outlive SubmitOp() and remain valid until the SQE reaches
  // the kernel, rather than living in Arm()/Update()'s stack frames.
  __kernel_timespec driver_timespec_{};
  __kernel_timespec fallback_timespec_{};
  __kernel_timespec update_timespec_{};
};

}  // namespace detail
}  // namespace coropact::luring
