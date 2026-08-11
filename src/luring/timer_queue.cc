// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include "coropact/luring/detail/timer_queue.h"

#include <liburing.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/luring/detail/loop_access.h"
#include "coropact/luring/loop.h"
#include "coropact/time/timestamp.h"

namespace coropact::luring::detail {

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
  DiscardAll();
  COROPACT_CHECK(timers_.Empty(), "LUringTimerQueue retained intrusive timer nodes");
  COROPACT_CHECK(active_.empty(), "LUringTimerQueue retained owned timers");
}

base::Result<time::TimerId> LUringTimerQueue::AddAfter(std::chrono::steady_clock::duration delay,
                                                       TimerCallback callback) {
  const auto now = time::Timestamp::Now();
  const auto seconds = std::chrono::duration<double>(delay).count();
  return AddTimer(std::move(callback), time::AddTime(now, std::max(0.0, seconds)));
}

base::Result<time::TimerId> LUringTimerQueue::AddTimer(TimerCallback callback,
                                                       time::Timestamp when) {
  COROPACT_CHECK(loop_ != nullptr, "LUringTimerQueue has no owner loop");
  COROPACT_CHECK(loop_->IsInLoopThread(), "LUringTimerQueue::AddTimer called from wrong thread");

  if (!when.Valid()) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }

  auto timer = std::make_unique<time::Timer>(std::move(callback), when, 0.0);
  const time::TimerId id{timer->sequence()};
  auto [it, inserted] = active_.emplace(id.sequence, std::move(timer));
  if (!inserted) {
    return std::unexpected(base::MakeErrno(EEXIST));
  }
  if (!timers_.Insert(it->second.get())) {
    active_.erase(it);
    return std::unexpected(base::MakeErrno(EEXIST));
  }

  auto reconciled = Reconcile();
  if (!reconciled.has_value()) {
    COROPACT_CHECK(timers_.Erase(it->second.get()),
                   "LUringTimerQueue inserted timer is missing from timer tree");
    active_.erase(it);
    return std::unexpected(reconciled.error());
  }
  return id;
}

base::Result<void> LUringTimerQueue::Cancel(time::TimerId id) noexcept {
  COROPACT_CHECK(loop_ != nullptr, "LUringTimerQueue has no owner loop");
  COROPACT_CHECK(loop_->IsInLoopThread(), "LUringTimerQueue::Cancel called from wrong thread");

  auto it = active_.find(id.sequence);
  if (it == active_.end()) {
    return std::unexpected(base::MakeErrno(ENOENT));
  }

  COROPACT_CHECK(timers_.Erase(it->second.get()),
                 "LUringTimerQueue active timer is missing from timer tree");
  active_.erase(it);
  // If the canceled timer was the driver deadline, leaving the old kernel
  // timeout in place is safe: it produces one harmless wakeup, after which
  // Reconcile arms the next deadline. Earlier deadlines still use update.
  ReconcileOrStop();
  return {};
}

void LUringTimerQueue::DiscardAll() noexcept {
  COROPACT_CHECK(loop_ != nullptr, "LUringTimerQueue has no owner loop");
  COROPACT_CHECK(loop_->IsInLoopThread(), "LUringTimerQueue::DiscardAll called from wrong thread");

  // TimerTree is intrusive: unlink every hook before destroying the owning
  // unique_ptrs. Clearing active_ first would leave TimerTree with dangling
  // nodes and make its destructor traverse freed storage.
  timers_.Clear();
  active_.clear();

  driver_armed_ = false;
  control_pending_ = false;
  control_is_fallback_ = false;
  fallback_armed_ = false;
  driver_deadline_ = time::Timestamp::Invalid();
  requested_deadline_ = time::Timestamp::Invalid();
}

void LUringTimerQueue::OnDriverComplete(LUringOp* op) noexcept {
  DriverOpHook::OwnerFrom(op)->HandleDriverComplete(op);
}

void LUringTimerQueue::OnControlComplete(LUringOp* op) noexcept {
  ControlOpHook::OwnerFrom(op)->HandleControlComplete(op);
}

void DispatchTimerDriverComplete(LUringOp* op) noexcept { LUringTimerQueue::OnDriverComplete(op); }

void DispatchTimerControlComplete(LUringOp* op) noexcept {
  LUringTimerQueue::OnControlComplete(op);
}

void LUringTimerQueue::HandleDriverComplete(LUringOp*) noexcept {
  driver_armed_ = false;
  driver_deadline_ = time::Timestamp::Invalid();
  ProcessExpired();
  ReconcileOrStop();
}

void LUringTimerQueue::HandleControlComplete(LUringOp* op) noexcept {
  control_pending_ = false;

  if (control_is_fallback_) {
    control_is_fallback_ = false;
    fallback_armed_ = false;
    ProcessExpired();
    ReconcileOrStop();
    return;
  }

  if (op->result.HasValue() && *op->result == 0) {
    driver_deadline_ = requested_deadline_;
  } else {
    // Some kernels/liburing combinations reject timeout updates with EINVAL.
    // Keep the original driver timeout in flight and use a second timeout as
    // a compatibility wakeup for the earlier deadline.
    timeout_update_supported_ = false;
  }
  ReconcileOrStop();
}

void LUringTimerQueue::ProcessExpired() noexcept {
  const auto now = time::Timestamp::Now();
  timers_.PopWhile([now](const time::Timer* timer) { return timer->expiration() <= now; },
                   [this](time::Timer* timer) noexcept {
                     const auto id = timer->sequence();
                     auto it = active_.find(id);
                     COROPACT_CHECK(it != active_.end(),
                                    "LUringTimerQueue expired timer is missing from active set");
                     std::unique_ptr<time::Timer> owned = std::move(it->second);
                     active_.erase(it);
                     owned->Run();
                   });
}

void LUringTimerQueue::ReconcileOrStop() noexcept {
  auto reconciled = Reconcile();
  if (!reconciled.has_value()) {
    loop_->RequestStop();
  }
}

base::Result<void> LUringTimerQueue::Reconcile() noexcept {
  if (control_pending_) return {};

  auto* earliest = timers_.Earliest();
  if (!driver_armed_) {
    if (earliest != nullptr) return Arm(earliest->expiration());
    return {};
  }

  if (earliest != nullptr && earliest->expiration() < driver_deadline_) {
    if (timeout_update_supported_) {
      return Update(earliest->expiration());
    }
    if (!fallback_armed_) {
      return ArmFallback(earliest->expiration());
    }
  }
  return {};
}

base::Result<void> LUringTimerQueue::Arm(time::Timestamp deadline) noexcept {
  driver_timespec_ = ToKernelTimespec(deadline);
  DriverOp()->BeginNextRequest();

  auto result = LoopAccess::SubmitOp(*loop_, DriverOp(), [this](io_uring_sqe* sqe) noexcept {
    io_uring_prep_timeout(sqe, &driver_timespec_, 0, IORING_TIMEOUT_ABS | IORING_TIMEOUT_REALTIME);
  });
  if (!result.has_value()) return std::unexpected(result.error());

  driver_armed_ = true;
  driver_deadline_ = deadline;
  return {};
}

base::Result<void> LUringTimerQueue::ArmFallback(time::Timestamp deadline) noexcept {
  fallback_timespec_ = ToKernelTimespec(deadline);
  ControlOp()->BeginNextRequest();
  control_is_fallback_ = true;

  auto result = LoopAccess::SubmitOp(*loop_, ControlOp(), [this](io_uring_sqe* sqe) noexcept {
    io_uring_prep_timeout(sqe, &fallback_timespec_, 0,
                          IORING_TIMEOUT_ABS | IORING_TIMEOUT_REALTIME);
  });
  if (result.has_value()) {
    control_pending_ = true;
    fallback_armed_ = true;
  } else {
    control_is_fallback_ = false;
    return std::unexpected(result.error());
  }
  return {};
}

base::Result<void> LUringTimerQueue::Update(time::Timestamp deadline) noexcept {
  update_timespec_ = ToKernelTimespec(deadline);
  requested_deadline_ = deadline;
  ControlOp()->BeginNextRequest();
  control_is_fallback_ = false;

  auto result = LoopAccess::SubmitOp(*loop_, ControlOp(), [this](io_uring_sqe* sqe) noexcept {
    io_uring_prep_timeout_update(sqe, &update_timespec_,
                                 reinterpret_cast<std::uint64_t>(DriverOp()),
                                 IORING_TIMEOUT_ABS | IORING_TIMEOUT_REALTIME);
  });
  if (!result.has_value()) {
    requested_deadline_ = time::Timestamp::Invalid();
    return std::unexpected(result.error());
  }
  control_pending_ = true;
  return {};
}

}  // namespace coropact::luring::detail
