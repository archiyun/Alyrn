// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include "coropact/luring/timer_queue.h"

#include <liburing.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <utility>

#include "coropact/luring/loop.h"
#include "coropact/time/timestamp.h"

namespace coropact::luring {

namespace {

__kernel_timespec ToKernelTimespec(time::Timestamp timestamp) noexcept {
  const auto micros = timestamp.MicrosecondsSinceEpoch();
  return __kernel_timespec{
      .tv_sec = static_cast<__kernel_time64_t>(micros / 1'000'000),
      .tv_nsec = static_cast<long>(micros % 1'000'000) * 1'000,
  };
}

}  // namespace

LUringTimerQueue::~LUringTimerQueue() noexcept {
  assert(timers_.empty());
  assert(active_.empty());
}

base::Result<time::TimerId> LUringTimerQueue::AddAfter(std::chrono::steady_clock::duration delay,
                                                       TimerCallback callback) {
  const auto now = time::Timestamp::Now();
  const auto seconds = std::chrono::duration<double>(delay).count();
  return AddTimer(std::move(callback), time::AddTime(now, std::max(0.0, seconds)));
}

base::Result<time::TimerId> LUringTimerQueue::AddTimer(TimerCallback callback,
                                                       time::Timestamp when) {
  assert(loop_ != nullptr);
  assert(loop_->IsInLoopThread());

  if (!when.Valid()) {
    return std::unexpected(base::make_errno(EINVAL));
  }

  auto timer = std::make_unique<time::Timer>(std::move(callback), when, 0.0);
  const time::TimerId id{timer->sequence()};
  auto [it, inserted] = active_.emplace(id.sequence, std::move(timer));
  if (!inserted) {
    return std::unexpected(base::make_errno(EEXIST));
  }
  if (!timers_.Insert(it->second.get())) {
    active_.erase(it);
    return std::unexpected(base::make_errno(EEXIST));
  }

  Reconcile();
  return id;
}

base::Result<void> LUringTimerQueue::Cancel(time::TimerId id) noexcept {
  assert(loop_ != nullptr);
  assert(loop_->IsInLoopThread());

  auto it = active_.find(id.sequence);
  if (it == active_.end()) {
    return std::unexpected(base::make_errno(ENOENT));
  }

  timers_.Erase(it->second.get());
  active_.erase(it);
  // If the canceled timer was the driver deadline, leaving the old kernel
  // timeout in place is safe: it produces one harmless wakeup, after which
  // Reconcile arms the next deadline. Earlier deadlines still use update.
  Reconcile();
  return {};
}

void LUringTimerQueue::OnDriverComplete(LUringOp* op) noexcept {
  static_cast<LUringTimerQueue*>(op->owner)->HandleDriverComplete(op);
}

void LUringTimerQueue::OnControlComplete(LUringOp* op) noexcept {
  static_cast<LUringTimerQueue*>(op->owner)->HandleControlComplete(op);
}

void LUringTimerQueue::HandleDriverComplete(LUringOp*) noexcept {
  driver_armed_ = false;
  driver_deadline_ = time::Timestamp::Invalid();
  ProcessExpired();
  Reconcile();
}

void LUringTimerQueue::HandleControlComplete(LUringOp* op) noexcept {
  control_pending_ = false;

  if (control_is_fallback_) {
    control_is_fallback_ = false;
    fallback_armed_ = false;
    ProcessExpired();
    Reconcile();
    return;
  }

  if (op->result.has_value() && *op->result == 0) {
    driver_deadline_ = requested_deadline_;
  } else {
    // Some kernels/liburing combinations reject timeout updates with EINVAL.
    // Keep the original driver timeout in flight and use a second timeout as
    // a compatibility wakeup for the earlier deadline.
    timeout_update_supported_ = false;
  }
  Reconcile();
}

void LUringTimerQueue::ProcessExpired() noexcept {
  const auto now = time::Timestamp::Now();
  timers_.PopWhile([now](const time::Timer* timer) { return timer->expiration() <= now; },
                   [this](time::Timer* timer) noexcept {
                     const auto id = timer->sequence();
                     auto it = active_.find(id);
                     if (it == active_.end()) return;
                     std::unique_ptr<time::Timer> owned = std::move(it->second);
                     active_.erase(it);
                     owned->Run();
                   });
}

void LUringTimerQueue::Reconcile() noexcept {
  if (control_pending_) return;

  auto* earliest = timers_.earliest();
  if (!driver_armed_) {
    if (earliest != nullptr) Arm(earliest->expiration());
    return;
  }

  if (earliest != nullptr && earliest->expiration() < driver_deadline_) {
    if (timeout_update_supported_) {
      Update(earliest->expiration());
    } else if (!fallback_armed_) {
      ArmFallback(earliest->expiration());
    }
  }
}

void LUringTimerQueue::Arm(time::Timestamp deadline) noexcept {
  driver_timespec_ = ToKernelTimespec(deadline);
  driver_op_.owner = this;
  driver_op_.on_complete = &LUringTimerQueue::OnDriverComplete;
  driver_op_.completed = false;
  driver_op_.resume_work.handle = {};

  auto result = loop_->SubmitOp(&driver_op_, [this](io_uring_sqe* sqe) noexcept {
    io_uring_prep_timeout(sqe, &driver_timespec_, 0, IORING_TIMEOUT_ABS | IORING_TIMEOUT_REALTIME);
  });
  if (!result.has_value()) return;

  driver_armed_ = true;
  driver_deadline_ = deadline;
}

void LUringTimerQueue::ArmFallback(time::Timestamp deadline) noexcept {
  fallback_timespec_ = ToKernelTimespec(deadline);
  control_op_.owner = this;
  control_op_.on_complete = &LUringTimerQueue::OnControlComplete;
  control_op_.completed = false;
  control_is_fallback_ = true;

  auto result = loop_->SubmitOp(&control_op_, [this](io_uring_sqe* sqe) noexcept {
    io_uring_prep_timeout(sqe, &fallback_timespec_, 0,
                          IORING_TIMEOUT_ABS | IORING_TIMEOUT_REALTIME);
  });
  if (result.has_value()) {
    control_pending_ = true;
    fallback_armed_ = true;
  } else {
    control_is_fallback_ = false;
  }
}

void LUringTimerQueue::Update(time::Timestamp deadline) noexcept {
  update_timespec_ = ToKernelTimespec(deadline);
  requested_deadline_ = deadline;
  control_op_.owner = this;
  control_op_.on_complete = &LUringTimerQueue::OnControlComplete;
  control_op_.completed = false;
  control_is_fallback_ = false;

  auto result = loop_->SubmitOp(&control_op_, [this](io_uring_sqe* sqe) noexcept {
    io_uring_prep_timeout_update(sqe, &update_timespec_,
                                 reinterpret_cast<std::uint64_t>(&driver_op_),
                                 IORING_TIMEOUT_ABS | IORING_TIMEOUT_REALTIME);
  });
  if (result.has_value()) control_pending_ = true;
}

}  // namespace coropact::luring
