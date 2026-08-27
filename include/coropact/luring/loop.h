// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <memory_resource>
#include <stop_token>
#include <utility>

#include "coropact/backend/loop.h"
#include "coropact/base/check.h"
#include "coropact/base/current_thread.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/work.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/detail/ring.h"
#include "coropact/luring/detail/sqe_prep.h"
#include "coropact/luring/options.h"
#include "coropact/result.h"
#include "coropact/time/clock.h"
#include "coropact/time/timer_id.h"
#include "coropact/time/timer_index_kind.h"

namespace coropact::luring {

class RecvSource;
namespace detail {
class LoopAccess;
class ProvidedBufferPool;
class TimerQueue;
}  // namespace detail

/*
 * Owner-thread io_uring dispatcher. Each loop owns one ring and submits,
 * receives CQEs, advances timers, and resumes coroutine work on that thread.
 * SQE/CQE decoding remains an implementation detail; callers only own
 * initialization, execution, timers, and scheduling.
 */
class Loop final : public coro::Scheduler {
public:
  COROPACT_DELETE_COPY_MOVE(Loop);

  // frame_resource is used for coroutine frames Scheduled by this loop.
  explicit Loop(std::pmr::memory_resource* frame_resource = nullptr);
  explicit Loop(time::TimerIndexKind timers,
                std::pmr::memory_resource* frame_resource = nullptr);

  // Initializes the underlying io_uring instance.
  // Must be called from the loop thread before Run().
  [[nodiscard]]
  Result<void> Init(const Options& options) noexcept;

  // The caller must drain user operation work before destruction. The
  // destructor never uses io_uring_queue_exit() as an implicit cancellation
  // mechanism for awaiter-owned operation storage.
  ~Loop() noexcept;

  [[nodiscard]]
  bool Initialized() const noexcept {
    return initialized_;
  }

  // Runs the dispatcher until RequestStop() or token cancellation. Run() is
  // owner-thread-only. Once stopping begins, it cancels and drains all
  // submitted ring operations before transitioning to Stopped. Application
  // objects still own descriptor destruction and their final Close() calls.
  void Run(std::stop_token token = {}) noexcept;

  // Requests dispatcher shutdown. This function is thread-safe, idempotent,
  // and wakes a blocked ring wait. It does not itself release user resources.
  void RequestStop() noexcept;

  [[nodiscard]]
  backend::LoopState State() const noexcept {
    return state_.load(std::memory_order_acquire);
  }

  [[nodiscard]]
  bool IsInLoopThread() const noexcept {
    return thread_id_ == base::CurrentThreadId();
  }

  [[nodiscard]]
  Result<time::TimerId> RunAfter(time::Duration delay, std::function<void()> callback);
  Result<void> CancelTimer(time::TimerId id) noexcept;

  // Enqueues coroutine work to be resumed by RunReady().
  void Schedule(coro::Work* work) noexcept override;

private:
  friend class detail::LoopAccess;
  friend class RecvSource;

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

  // Enqueues work produced by a CQE or timeout. These
  // works receive bounded priority over ordinary ready work.
  void ScheduleCompletion(coro::Work* work) noexcept;

  // Prepares one io_uring operation. The operation reaches the kernel only
  // after FlushSubmit() or another submission path.
  template <class Prep>
  [[nodiscard]]
  Result<void> SubmitOp(detail::Op* op, Prep&& prep) noexcept {
    COROPACT_CHECK(IsInLoopThread(), "Loop::SubmitOp called from wrong thread");

    if (!initialized_) {
      return std::unexpected(Errno(EBADF));
    }
    if (op == nullptr) {
      return std::unexpected(Errno(EINVAL));
    }
    const backend::LoopState state = State();
    if ((state == backend::LoopState::kStopping || state == backend::LoopState::kStopped) &&
        op != &cancel_all_op_ && op != &wake_op_) {
      return std::unexpected(Errno(ECANCELED));
    }

    io_uring_sqe* sqe = ring_.GetSqe();
    if (sqe == nullptr) {
      auto flushed = FlushSubmit();
      if (!flushed.has_value()) {
        return flushed;
      }

      sqe = ring_.GetSqe();
      if (sqe == nullptr) {
        return std::unexpected(Errno(ENOSPC));
      }
    }

    prep(sqe);
    io_uring_sqe_set_data(sqe, op);
    ++pending_submit_;
    return {};
  }

  [[nodiscard]]
  Result<void> FlushSubmit() noexcept;
  // Cancels all user operations currently pending in this ring. The resulting
  // CQEs are still delivered through the normal completion path so awaiters
  // can release their stream ownership before the ring is destroyed.
  [[nodiscard]]
  Result<void> CancelPendingOperations() noexcept;
  [[nodiscard]]
  Result<std::size_t> PollCompletions() noexcept;
  [[nodiscard]]
  Result<std::size_t> WaitCompletions() noexcept;

  void RunReady() noexcept;

  [[nodiscard]]
  Result<detail::ProvidedBufferPool*> GetSharedProvidedBufferPool(
      std::size_t buffer_size, std::size_t source_capacity) noexcept;

  [[nodiscard]]
  Result<std::uint16_t> AllocateBufferGroupId() noexcept {
    if (next_buffer_group_id_ > std::numeric_limits<std::uint16_t>::max()) {
      return std::unexpected(Errno(EOVERFLOW));
    }
    return static_cast<std::uint16_t>(next_buffer_group_id_++);
  }

  [[nodiscard]]
  Result<std::size_t> WaitCompletionsFor(std::chrono::nanoseconds timeout) noexcept;

  void DrainStoppedOperations() noexcept;
  void HandleCqe(io_uring_cqe* cqe) noexcept;

  const base::ThreadId thread_id_;
  detail::Ring ring_;
  coro::WorkQueue ready_;
  coro::WorkQueue completion_ready_;
  bool initialized_{false};

  // Prepared SQEs that have not yet produced a CQE.
  std::size_t pending_submit_{0};

  // Submitted operations that have not yet produced a CQE.
  std::size_t inflight_{0};

  [[nodiscard]]
  bool HasReadyWork() const noexcept {
    return !ready_.Empty() || !completion_ready_.Empty();
  }

  [[nodiscard]]
  Result<void> ArmWakePoll() noexcept;
  void DrainWakeFd() noexcept;
  void Wake() noexcept;

  // Cross-thread lifecycle state observed by the event loop.
  std::atomic<backend::LoopState> state_{backend::LoopState::kCreated};

  std::unique_ptr<detail::TimerQueue> timers_;
  int wake_fd_{-1};
  bool wake_pending_{false};
  bool wake_inflight_{false};
  detail::Op wake_op_{detail::OpKind::kWake};
  bool cancel_all_pending_{false};
  detail::Op cancel_all_op_{detail::OpKind::kCancelAll};
  std::uint32_t next_buffer_group_id_{1};
  std::unique_ptr<detail::ProvidedBufferPool> shared_buffer_pool_;
  std::size_t shared_buffer_capacity_{0};
  std::size_t shared_buffer_size_{0};
};

}  // namespace coropact::luring
