// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <liburing.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>

#include "coropact/base/error.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/detail/op_hook.h"
#include "coropact/time/timer.h"
#include "coropact/time/timer_id.h"
#include "coropact/time/timer_tree.h"

namespace coropact::luring {

class LUringLoop;

namespace detail {

struct TimerDriverTag;
struct TimerControlTag;

// One timer queue belongs to one LUringLoop and is only accessed by that
// loop's thread. The timer tree stays in user space; one io_uring timeout is
// used to wake the loop for the earliest timer.
class LUringTimerQueue final : public LUringOpHook<LUringTimerQueue, TimerDriverTag>,
                               public LUringOpHook<LUringTimerQueue, TimerControlTag> {
  friend void DispatchTimerDriverComplete(LUringOp* op) noexcept;
  friend void DispatchTimerControlComplete(LUringOp* op) noexcept;

public:
  using DriverOpHook = LUringOpHook<LUringTimerQueue, TimerDriverTag>;
  using ControlOpHook = LUringOpHook<LUringTimerQueue, TimerControlTag>;
  using TimerCallback = std::function<void()>;

  explicit LUringTimerQueue(LUringLoop* loop) noexcept
      : DriverOpHook(LUringOpKind::kTimerDriverComplete),
        ControlOpHook(LUringOpKind::kTimerControlComplete),
        loop_(loop) {}
  ~LUringTimerQueue() noexcept;

  LUringTimerQueue(const LUringTimerQueue&) = delete;
  LUringTimerQueue& operator=(const LUringTimerQueue&) = delete;

  [[nodiscard]]
  base::Result<time::TimerId> AddAfter(std::chrono::steady_clock::duration delay,
                                       TimerCallback callback);
  [[nodiscard]]
  base::Result<time::TimerId> AddTimer(TimerCallback callback, time::Timestamp when);
  base::Result<void> Cancel(time::TimerId id) noexcept;

  // Drops every logical timer without invoking user callbacks. The loop calls
  // this only after all submitted timer requests have reached a physical
  // terminal CQE. The destructor repeats the operation defensively so the
  // intrusive tree never outlives the Timer objects owned by active_.
  void DiscardAll() noexcept;

private:
  static void OnDriverComplete(LUringOp* op) noexcept;
  static void OnControlComplete(LUringOp* op) noexcept;

  void HandleDriverComplete(LUringOp* op) noexcept;
  void HandleControlComplete(LUringOp* op) noexcept;
  void ProcessExpired() noexcept;
  void ReconcileOrStop() noexcept;
  [[nodiscard]] base::Result<void> Reconcile() noexcept;
  [[nodiscard]] base::Result<void> Arm(time::Timestamp deadline) noexcept;
  [[nodiscard]] base::Result<void> ArmFallback(time::Timestamp deadline) noexcept;
  [[nodiscard]] base::Result<void> Update(time::Timestamp deadline) noexcept;

  LUringOp* DriverOp() noexcept { return DriverOpHook::Op(); }
  LUringOp* ControlOp() noexcept { return ControlOpHook::Op(); }

  LUringLoop* loop_;
  time::TimerTree timers_;
  std::unordered_map<std::int64_t, std::unique_ptr<time::Timer>> active_;

  bool driver_armed_{false};
  bool control_pending_{false};
  bool control_is_fallback_{false};
  bool fallback_armed_{false};
  bool timeout_update_supported_{true};
  time::Timestamp driver_deadline_;
  time::Timestamp requested_deadline_;

  // io_uring_prep_timeout stores a user pointer in the SQE. These objects
  // must therefore outlive SubmitOp() and remain valid until the SQE reaches
  // the kernel, rather than living in Arm()/Update()'s stack frames.
  __kernel_timespec driver_timespec_{};
  __kernel_timespec fallback_timespec_{};
  __kernel_timespec update_timespec_{};
};

}  // namespace detail
}  // namespace coropact::luring
