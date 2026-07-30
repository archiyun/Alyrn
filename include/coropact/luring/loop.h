// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <new>
#include <limits>
#include <optional>
#include <stop_token>
#include <utility>

#include "coropact/base/current_thread.h"
#include "coropact/base/error.h"
#include "coropact/base/try.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/work.h"
#include "coropact/luring/capability.h"
#include "coropact/luring/mailbox.h"
#include "coropact/luring/op.h"
#include "coropact/luring/options.h"
#include "coropact/luring/ring.h"
#include "coropact/luring/timer_queue.h"
#include "coropact/time/timer_id.h"

namespace coropact::luring {

class LUringRecvSource;

// Single-threaded io_uring event loop
//
// Each LUringLoop owns one LUringRing and is bound to the thread that creates
// it. IO operations are submitted to the ring, and completed operations resume
// their coroutine work through the Scheduler interface.
//
// Notify function:
//   target.PostMessage() -> source.Notify() -> target.HandleCqe() ->
//   target.ScheduleCompletion(work)
class LUringLoop final : public coro::Scheduler {
public:
  COROPACT_DELETE_COPY_MOVE(LUringLoop);

  // frame_resource is used for coroutine frames Scheduled by this loop.
  explicit LUringLoop(std::pmr::memory_resource* frame_resource = nullptr);

  // Initializes the underlying io_uring instance.
  // Must be called from the loop thread before Loop().
  [[nodiscard]]
  base::Result<void> Init(const LUringOptions& options) noexcept {
    return Init(options, options.active_profile);
  }

  // Explicit profile overload used by callers that keep loop configuration
  // separate from the backend binding request.
  [[nodiscard]]
  base::Result<void> Init(
      const LUringOptions& options,
      RuntimeProfile active_profile) noexcept;

  ~LUringLoop() noexcept;

  [[nodiscard]]
  bool Initialized() const noexcept {
    return initialized_;
  }

  // An explicit extension requires both a startup profile request and
  // support reported by the actual ring probe.
  [[nodiscard]]
  bool HasCapability(NativeFeature feature) const noexcept {
    return binding_.has_value() &&
           binding_->active_profile.Has(feature) &&
           binding_->capabilities.Has(feature);
  }

  // Runs the event loop until cancellation or Quit().
  void Loop(std::stop_token token) noexcept;

  // Requests the event loop to exit.
  // This function may be called from another thread.
  void Quit() noexcept;

  [[nodiscard]]
  bool IsInLoopThread() const noexcept {
    return thread_id_ == base::tid();
  }
  [[nodiscard]]
  int ThreadId() const noexcept {
    return thread_id_;
  }

  [[nodiscard]]
  int RingFd() const noexcept {
    return ring_.Fd();
  }

  // Internal wake polling is not part of the user-visible operation count.
  [[nodiscard]]
  std::size_t PendingSubmitCount() const noexcept {
    return pending_submit_ - (wake_pending_ ? 1 : 0);
  }

  [[nodiscard]]
  std::size_t InflightCount() const noexcept {
    return inflight_ - (wake_inflight_ ? 1 : 0);
  }

  [[nodiscard]]
  bool IsDrained() const noexcept {
    return !HasReadyWork() && PendingSubmitCount() == 0 && InflightCount() == 0;
  }

#if defined(COROPACT_ENABLE_TEST_HOOKS)
  // Test-only deterministic failure injection. It is intentionally kept out
  // of normal builds so production LUringLoop has no fault-injection state.
  void FailNextSubmissionsForTesting(
      std::size_t count,
      int error = EIO) noexcept {
    assert(error > 0);
    test_submit_failures_ = count;
    test_successful_submissions_before_failure_ = 0;
    test_submit_error_ = error;
  }

  // Fails one submission after exactly `successful_submissions` test-visible
  // submissions have succeeded. This makes linked-operation tests able to
  // target their second SQE without failing the first physical request.
  void FailSubmissionAfterForTesting(
      std::size_t successful_submissions,
      int error = EIO) noexcept {
    assert(error > 0);
    test_submit_failures_ = 1;
    test_successful_submissions_before_failure_ = successful_submissions;
    test_submit_error_ = error;
  }
#endif

  [[nodiscard]]
  base::Result<time::TimerId> RunAfter(std::chrono::steady_clock::duration delay,
                                                     LUringTimerQueue::TimerCallback callback) {
    assert(IsInLoopThread());
    if (!initialized_) {
      return std::unexpected(base::MakeErrno(EBADF));
    }
    return timers_.AddAfter(delay, std::move(callback));
  }

  base::Result<void> CancelTimer(time::TimerId id) noexcept {
    assert(IsInLoopThread());
    if (!initialized_) {
      return std::unexpected(base::MakeErrno(EBADF));
    }
    return timers_.Cancel(id);
  }

  // Enqueues coroutine work to be resumed by RunReady().
  void Schedule(coro::Work* work) noexcept override;

  // Enqueues work produced by a CQE, timeout, or mailbox completion. These
  // works receive bounded priority over ordinary ready work.
  void ScheduleCompletion(coro::Work* work) noexcept;

  // Thread-safe enqueue
  // The event loop is not woken yet; msg_ring will provide notification later.
  [[nodiscard]]
  LUringMailboxPushResult PostMessage(LUringMessage message) {
    return mailbox_.Push(std::move(message));
  }

  // Re-arms notification after a source-side msg_ring submission failure.
  [[nodiscard]]
  bool RetryMessageNotification() noexcept { return mailbox_.RetryNotification(); }

  template <class F>
  std::size_t DrainMessages(F&& handler) {
    assert(IsInLoopThread());
    return mailbox_.Drain(std::forward<F>(handler));
  }

  // Prepares one io_uring operation.
  //
  // State transition:
  //   free SQE -> prepared SQE -> pending_submit_
  //
  // The operation is not guaranteed to reach the kernel until FlushSubmit()
  // or another submission path is executed.
  template <class Prep>
  [[nodiscard]]
  base::Result<void> SubmitOp(LUringOp* op, Prep&& prep) noexcept {
    assert(IsInLoopThread());

    if (!initialized_) {
      return std::unexpected(base::MakeErrno(EBADF));
    }
    if (op == nullptr) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }

#if defined(COROPACT_ENABLE_TEST_HOOKS)
    if (test_submit_failures_ != 0) {
      if (test_successful_submissions_before_failure_ != 0) {
        --test_successful_submissions_before_failure_;
      } else {
        --test_submit_failures_;
        return std::unexpected(base::MakeErrno(test_submit_error_));
      }
    }
#endif

    io_uring_sqe* sqe = ring_.GetSqe();
    if (sqe == nullptr) {
      COROPACT_TRY(FlushSubmit());

      sqe = ring_.GetSqe();
      if (sqe == nullptr) {
        return std::unexpected(base::MakeErrno(ENOSPC));
      }
    }

    prep(sqe);
    io_uring_sqe_set_data(sqe, op);
    ++pending_submit_;
    return {};
  }

  [[nodiscard]]
  base::Result<void> SubmitMsgRing(LUringOp* op, int target_ring_fd,
                                                 std::uint32_t type) noexcept {
    assert(IsInLoopThread());

    if (target_ring_fd < 0) {
      return std::unexpected(base::MakeErrno(EBADF));
    }

    return SubmitOp(op, [this, target_ring_fd, type](io_uring_sqe* sqe) noexcept {
      ring_.PrepMsgRing(sqe, target_ring_fd, type, kMsgRingNotificationUserData);
    });
  }

  [[nodiscard]]
  base::Result<void> Notify(LUringLoop& target, LUringOp* op) noexcept {
    assert(IsInLoopThread());

    if (op == nullptr) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }

    return SubmitMsgRing(op, target.RingFd(), 0);
  }

  [[nodiscard]]
  base::Result<void> FlushSubmit() noexcept;
  // Cancels all user operations currently pending in this ring. The resulting
  // CQEs are still delivered through the normal completion path so awaiters
  // can release their stream ownership before the ring is destroyed.
  [[nodiscard]]
  base::Result<void> CancelPendingOperations() noexcept;
  [[nodiscard]]
  base::Result<std::size_t> PollCompletions() noexcept;
  [[nodiscard]]
  base::Result<std::size_t> WaitCompletions() noexcept;

  void RunReady() noexcept;
  void RunUntilIdle();

private:
  friend class LUringRecvSource;

  [[nodiscard]]
  base::Result<std::uint16_t> AllocateBufferGroupId() noexcept {
    if (next_buffer_group_id_ > std::numeric_limits<std::uint16_t>::max()) {
      return std::unexpected(base::MakeErrno(EOVERFLOW));
    }
    return static_cast<std::uint16_t>(next_buffer_group_id_++);
  }

  [[nodiscard]]
  base::Result<std::size_t> WaitCompletionsFor(
      std::chrono::nanoseconds timeout) noexcept;

  void HandleCqe(io_uring_cqe* cqe) noexcept;
  void HandleMailbox() noexcept;

  const int thread_id_;
  LUringRing ring_;
  coro::WorkQueue ready_;
  coro::WorkQueue completion_ready_;
  bool initialized_{false};
  std::optional<RuntimeBinding> binding_;

  // Prepared SQEs that have not yet produced a CQE.
  std::size_t pending_submit_{0};

  // Submitted operations that have not yet produced a CQE.
  std::size_t inflight_{0};

  // Preferred number of prepared operations before performing a batch submit.
  std::size_t submit_batch_{32};

  // Fairness budget for one RunReady() pass. Zero means unlimited.
  std::size_t max_ready_work_per_turn_{256};

  // Completion budget for one PollCompletions() or WaitCompletionsFor() pass.
  // Zero means unlimited.
  std::size_t max_cqe_per_turn_{256};

  // Wall-clock fairness budget for one RunReady() pass. Zero means unlimited.
  std::chrono::microseconds max_ready_time_per_turn_{50};

  // Completion-ready sub-budget for one RunReady() pass. Zero means unlimited.
  std::size_t max_completion_work_per_turn_{64};

  // Age threshold for promoting completion-ready work to the urgent budget.
  std::chrono::microseconds completion_queue_age_threshold_{0};

  // Bounded completion budget used after age-based promotion.
  std::size_t max_urgent_completion_work_per_turn_{80};

  // Age threshold for suppressing completion promotion when normal work is
  // already overdue.
  std::chrono::microseconds normal_queue_age_threshold_{5000};

  std::size_t ready_depth_{0};
  std::size_t completion_ready_depth_{0};
  std::uint64_t ready_nonempty_since_ns_{0};
  std::uint64_t completion_ready_nonempty_since_ns_{0};

  [[nodiscard]]
  bool HasReadyWork() const noexcept {
    return !ready_.Empty() || !completion_ready_.Empty();
  }

  [[nodiscard]]
  base::Result<void> ArmWakePoll() noexcept;
  void DrainWakeFd() noexcept;
  void Wake() noexcept;

  // Cross-thread exit request observed by the event loop.
  std::atomic_bool quit_{false};

  LUringMailbox mailbox_;
  LUringTimerQueue timers_;
  int wake_fd_{-1};
  bool wake_pending_{false};
  bool wake_inflight_{false};
  LUringOp wake_op_{LUringOpKind::kWake};
  bool cancel_all_pending_{false};
  LUringOp cancel_all_op_{LUringOpKind::kCancelAll};
  std::uint32_t next_buffer_group_id_{1};

#if defined(COROPACT_ENABLE_TEST_HOOKS)
  std::size_t test_submit_failures_{0};
  std::size_t test_successful_submissions_before_failure_{0};
  int test_submit_error_{EIO};
#endif
};

}  // namespace coropact::luring
