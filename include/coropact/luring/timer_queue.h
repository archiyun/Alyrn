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
#include "coropact/luring/op.h"
#include "coropact/time/timer.h"
#include "coropact/time/timer_id.h"
#include "coropact/time/timer_tree.h"

namespace coropact::luring {

class LUringLoop;

// One timer queue belongs to one LUringLoop and is only accessed by that
// loop's thread. The timer tree stays in user space; one io_uring timeout is
// used to wake the loop for the earliest timer.
class LUringTimerQueue final {
public:
  using TimerCallback = std::function<void()>;

  explicit LUringTimerQueue(LUringLoop* loop) noexcept : loop_(loop) {}
  ~LUringTimerQueue() noexcept;

  LUringTimerQueue(const LUringTimerQueue&) = delete;
  LUringTimerQueue& operator=(const LUringTimerQueue&) = delete;

  [[nodiscard]] base::Result<time::TimerId> AddAfter(std::chrono::steady_clock::duration delay,
                                                     TimerCallback callback);
  [[nodiscard]] base::Result<time::TimerId> AddTimer(TimerCallback callback, time::Timestamp when);
  base::Result<void> Cancel(time::TimerId id) noexcept;

private:
  static void OnDriverComplete(LUringOp* op) noexcept;
  static void OnControlComplete(LUringOp* op) noexcept;

  void HandleDriverComplete(LUringOp* op) noexcept;
  void HandleControlComplete(LUringOp* op) noexcept;
  void ProcessExpired() noexcept;
  void Reconcile() noexcept;
  void Arm(time::Timestamp deadline) noexcept;
  void ArmFallback(time::Timestamp deadline) noexcept;
  void Update(time::Timestamp deadline) noexcept;

  LUringLoop* loop_;
  time::TimerTree timers_;
  std::unordered_map<std::int64_t, std::unique_ptr<time::Timer>> active_;

  LUringOp driver_op_{.kind = LUringOpKind::kTimeout};
  LUringOp control_op_{.kind = LUringOpKind::kTimeout};
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

}  // namespace coropact::luring
