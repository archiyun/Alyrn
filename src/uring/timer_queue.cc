// SPDX-License-Identifier: MIT

#include "alyrn/detail/uring/timer_queue.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <utility>

#include "alyrn/detail/base/check.h"
#include "alyrn/detail/uring/loop_access.h"
#include "alyrn/detail/uring/sqe_prep.h"
#include "alyrn/uring/loop.h"
#include "alyrn/time/clock.h"

namespace alyrn::uring::detail {

namespace {

__kernel_timespec ToKernelTimespec(time::Deadline deadline) noexcept {
  const auto elapsed = deadline.time_since_epoch();
  const auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
  const auto clamped = std::max<std::int64_t>(0, nanoseconds);
  return __kernel_timespec{
      .tv_sec = static_cast<__kernel_time64_t>(clamped / 1'000'000'000),
      .tv_nsec = static_cast<long>(clamped % 1'000'000'000),
  };
}

}  // namespace

TimerQueue::~TimerQueue() noexcept {
  DiscardAll();
  ALYRN_CHECK(timers_.Empty(), "TimerQueue retained intrusive timer nodes");
  ALYRN_CHECK(active_.empty(), "TimerQueue retained owned timers");
}

Result<time::TimerId> TimerQueue::AddAfter(time::Duration delay,
                                                       TimerCallback callback) {
  return AddTimer(std::move(callback), time::SteadyNow() + std::max(delay, time::Duration::zero()));
}

Result<time::TimerId> TimerQueue::AddTimer(TimerCallback callback,
                                                       time::Deadline deadline) {
  ALYRN_CHECK(loop_ != nullptr, "TimerQueue has no owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "TimerQueue::AddTimer called from wrong thread");

  auto timer = std::make_unique<::alyrn::detail::time::Timer>(
      std::move(callback), deadline, time::Duration::zero());
  const time::TimerId id{timer->sequence()};
  auto [it, inserted] = active_.emplace(id.sequence, std::move(timer));
  if (!inserted) {
    return std::unexpected(Errno(EEXIST));
  }
  if (!timers_.Insert(it->second.get())) {
    active_.erase(it);
    return std::unexpected(Errno(EEXIST));
  }

  auto reconciled = Reconcile();
  if (!reconciled.has_value()) {
    ALYRN_CHECK(timers_.Erase(it->second.get()),
                   "TimerQueue inserted timer is missing from timer tree");
    active_.erase(it);
    return std::unexpected(reconciled.error());
  }
  return id;
}

Result<void> TimerQueue::Cancel(time::TimerId id) noexcept {
  ALYRN_CHECK(loop_ != nullptr, "TimerQueue has no owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "TimerQueue::Cancel called from wrong thread");

  auto it = active_.find(id.sequence);
  if (it == active_.end()) {
    return std::unexpected(Errno(ENOENT));
  }

  ALYRN_CHECK(timers_.Erase(it->second.get()),
                 "TimerQueue active timer is missing from timer tree");
  active_.erase(it);
  // If the canceled timer was the driver deadline, leaving the old kernel
  // timeout in place is safe: it produces one harmless wakeup, after which
  // Reconcile arms the next deadline. Earlier deadlines still use update.
  ReconcileOrStop();
  return {};
}

void TimerQueue::DiscardAll() noexcept {
  ALYRN_CHECK(loop_ != nullptr, "TimerQueue has no owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "TimerQueue::DiscardAll called from wrong thread");

  // TimerIndex is intrusive: unlink every hook before destroying the owning
  // unique_ptrs. Clearing active_ first would leave TimerIndex with dangling
  // nodes and make its destructor traverse freed storage.
  timers_.Clear();
  active_.clear();

  driver_armed_ = false;
  control_pending_ = false;
  control_is_fallback_ = false;
  fallback_armed_ = false;
  driver_deadline_ = {};
  requested_deadline_ = {};
}

void TimerQueue::OnDriverComplete(::alyrn::uring::detail::Op* op) noexcept {
  DriverOpHook::OwnerFrom(op)->HandleDriverComplete(op);
}

void TimerQueue::OnControlComplete(::alyrn::uring::detail::Op* op) noexcept {
  ControlOpHook::OwnerFrom(op)->HandleControlComplete(op);
}

void DispatchTimerDriverComplete(::alyrn::uring::detail::Op* op) noexcept { TimerQueue::OnDriverComplete(op); }

void DispatchTimerControlComplete(::alyrn::uring::detail::Op* op) noexcept {
  TimerQueue::OnControlComplete(op);
}

void TimerQueue::HandleDriverComplete(::alyrn::uring::detail::Op* op) noexcept {
  ALYRN_CHECK(op->result.HasValue(), "timer driver CQE has no result");
  const bool expired = *op->result == -ETIME;
  driver_armed_ = false;
  driver_deadline_ = {};
  // A timeout update may first terminate the prior physical request with
  // ECANCELED. That CQE only closes the old driver request; it must not be
  // interpreted as a logical timer expiry. Reconcile() then arms the earliest
  // remaining Deadline as a fresh physical request.
  if (expired) {
    ProcessExpired();
  }
  ReconcileOrStop();
}

void TimerQueue::HandleControlComplete(::alyrn::uring::detail::Op* op) noexcept {
  ALYRN_CHECK(op->result.HasValue(), "timer control CQE has no result");
  control_pending_ = false;

  if (control_is_fallback_) {
    control_is_fallback_ = false;
    fallback_armed_ = false;
    if (*op->result == -ETIME) {
      ProcessExpired();
    }
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

void TimerQueue::ProcessExpired() noexcept {
  const auto now = time::SteadyNow();
  timers_.PopWhile(
      [now](const ::alyrn::detail::time::Timer* timer) { return timer->expiration() <= now; },
      [this](::alyrn::detail::time::Timer* timer) noexcept {
                     const auto id = timer->sequence();
                     auto it = active_.find(id);
                     ALYRN_CHECK(it != active_.end(),
                                    "TimerQueue expired timer is missing from active set");
                     std::unique_ptr<::alyrn::detail::time::Timer> owned = std::move(it->second);
                     active_.erase(it);
                     owned->Run();
                   });
}

void TimerQueue::ReconcileOrStop() noexcept {
  auto reconciled = Reconcile();
  if (!reconciled.has_value()) {
    loop_->RequestStop();
  }
}

Result<void> TimerQueue::Reconcile() noexcept {
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

Result<void> TimerQueue::Arm(time::Deadline deadline) noexcept {
  driver_timespec_ = ToKernelTimespec(deadline);
  DriverOp()->BeginNextRequest();

  auto result = LoopAccess::SubmitOp(
      *loop_, DriverOp(), detail::PrepareAbsoluteTimeout(&driver_timespec_));
  if (!result.has_value()) return std::unexpected(result.error());

  driver_armed_ = true;
  driver_deadline_ = deadline;
  return {};
}

Result<void> TimerQueue::ArmFallback(time::Deadline deadline) noexcept {
  fallback_timespec_ = ToKernelTimespec(deadline);
  ControlOp()->BeginNextRequest();
  control_is_fallback_ = true;

  auto result = LoopAccess::SubmitOp(
      *loop_, ControlOp(), detail::PrepareAbsoluteTimeout(&fallback_timespec_));
  if (result.has_value()) {
    control_pending_ = true;
    fallback_armed_ = true;
  } else {
    control_is_fallback_ = false;
    return std::unexpected(result.error());
  }
  return {};
}

Result<void> TimerQueue::Update(time::Deadline deadline) noexcept {
  update_timespec_ = ToKernelTimespec(deadline);
  requested_deadline_ = deadline;
  ControlOp()->BeginNextRequest();
  control_is_fallback_ = false;

  auto result = LoopAccess::SubmitOp(
      *loop_, ControlOp(), detail::PrepareAbsoluteTimeoutUpdate(
                               &update_timespec_, reinterpret_cast<std::uint64_t>(DriverOp())));
  if (!result.has_value()) {
    requested_deadline_ = {};
    return std::unexpected(result.error());
  }
  control_pending_ = true;
  return {};
}

}  // namespace alyrn::uring::detail
