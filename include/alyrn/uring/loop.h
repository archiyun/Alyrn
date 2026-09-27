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
#include <mutex>
#include <stop_token>
#include <utility>
#include <vector>

#include "alyrn/backend/loop.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/work.h"
#include "alyrn/detail/check.h"
#include "alyrn/detail/loop_shutdown.h"
#include "alyrn/result.h"
#include "alyrn/time/clock.h"
#include "alyrn/time/timer_id.h"
#include "alyrn/uring/detail/op.h"
#include "alyrn/uring/detail/ring.h"
#include "alyrn/uring/detail/sqe_prep.h"
#include "alyrn/uring/options.h"

namespace alyrn::uring {

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
  ALYRN_DELETE_COPY_MOVE(Loop);

  // frame_resource is used for coroutine frames Scheduled by this loop.
  explicit Loop(std::pmr::memory_resource* frame_resource = nullptr);

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

  // Thread-safe. Queues callback to run on the owner thread, inside this
  // Loop's scheduling context, on a later turn; callbacks posted from one
  // thread run in the order they were posted. A successful Post runs exactly
  // once: callbacks posted before the Loop finishes its shutdown drain still
  // run during that drain. Afterwards Post returns operation_canceled and the
  // callback is destroyed, unrun, on the calling thread. The Loop must outlive
  // every Post call; a Loop destroyed without running destroys queued
  // callbacks unrun. Callbacks run during shutdown must not start new I/O.
  [[nodiscard]]
  Result<void> Post(std::function<void()> callback);

  [[nodiscard]]
  backend::LoopState State() const noexcept {
    return state_.load(std::memory_order_acquire);
  }

  [[nodiscard]]
  bool IsInLoopThread() const noexcept;

  // Runs callback immediately on the owning loop thread, inside this Loop's
  // scheduling context: it may use owner-affine Channels and Spawn.
  void RunOnOwner(std::function<void()> callback) noexcept;

  // Timers run callbacks on the owner thread inside this Loop's scheduling
  // context, so they may use owner-affine Channels and Spawn. The spelling
  // matches epoll::Loop; registration returns Result here because arming the
  // earliest deadline submits a ring request, which can fail.
  //
  // Runs callback once after delay.
  [[nodiscard]]
  Result<time::TimerId> RunAfter(time::Duration delay, std::function<void()> callback);
  // Runs callback once at deadline.
  [[nodiscard]]
  Result<time::TimerId> RunAt(time::Deadline deadline, std::function<void()> callback);
  // Runs callback every interval, starting one interval from now, until
  // Cancel(); a callback may cancel its own timer. A nonpositive interval runs
  // it once.
  [[nodiscard]]
  Result<time::TimerId> RunEvery(time::Duration interval, std::function<void()> callback);
  // Cancels a pending timer. Returns ENOENT for a one-shot timer that already
  // ran or an id that was never issued.
  Result<void> Cancel(time::TimerId id) noexcept;
  // Same as Cancel().
  Result<void> CancelTimer(time::TimerId id) noexcept;

  // Enqueues coroutine work to be resumed by RunReady().
  void Schedule(coro::Work* work) noexcept override;

private:
  friend class detail::LoopAccess;
  friend class RecvSource;

  int RingFd() const noexcept { return ring_.Fd(); }

  // Internal wake polling is not part of the user-visible operation count.
  std::size_t PendingSubmitCount() const noexcept {
    return pending_submit_ - (wake_pending_ ? 1 : 0);
  }

  std::size_t InflightCount() const noexcept { return inflight_ - (wake_inflight_ ? 1 : 0); }

  bool IsDrained() const noexcept {
    return !HasReadyWork() && PendingSubmitCount() == 0 && InflightCount() == 0;
  }

  // Enqueues work produced by a CQE or timeout. These
  // works receive bounded priority over ordinary ready work.
  void ScheduleCompletion(coro::Work* work) noexcept;

  // Prepares one io_uring operation. The operation reaches the kernel only
  // after FlushSubmit() or another submission path.
  template <class Prep>
  Result<void> SubmitOp(detail::Op* op, Prep&& prep) noexcept {
    ALYRN_CHECK(IsInLoopThread(), "Loop::SubmitOp called from wrong thread");

    if (!initialized_) {
      return std::unexpected(Errno(EBADF));
    }
    if (op == nullptr) {
      return std::unexpected(Errno(EINVAL));
    }
    const backend::LoopState state = State();
    const auto kind = op->DispatchKind();
    const bool is_stream_deadline_cancel = kind == detail::OpKind::kStreamReadCancelComplete ||
                                           kind == detail::OpKind::kStreamWriteCancelComplete;
    if ((state == backend::LoopState::kStopping || state == backend::LoopState::kStopped) &&
        op != &cancel_all_op_ && op != &wake_op_ && !is_stream_deadline_cancel) {
      return std::unexpected(Errno(ECANCELED));
    }

    io_uring_sqe* sqe = ring_.GetSqe();
    if (sqe == nullptr) {
      auto flushed = FlushSubmit();
      if (!flushed.HasValue()) {
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

  Result<void> FlushSubmit() noexcept;
  // Cancels all user operations currently pending in this ring. The resulting
  // CQEs are still delivered through the normal completion path so awaiters
  // can release their stream ownership before the ring is destroyed.
  Result<void> CancelPendingOperations() noexcept;
  Result<std::size_t> PollCompletions() noexcept;
  Result<std::size_t> WaitCompletions() noexcept;

  void RunReady() noexcept;
  void RunPosted() noexcept;
  void DrainPostedAndClose() noexcept;

  Result<detail::ProvidedBufferPool*> GetSharedProvidedBufferPool(
      std::size_t buffer_size, std::size_t source_capacity) noexcept;
  Result<detail::ProvidedBufferPool*> GetSharedProvidedBufferPool(
      std::size_t source_capacity) noexcept;

  Result<std::uint16_t> AllocateBufferGroupId() noexcept {
    if (next_buffer_group_id_ > std::numeric_limits<std::uint16_t>::max()) {
      return std::unexpected(Errno(EOVERFLOW));
    }
    return static_cast<std::uint16_t>(next_buffer_group_id_++);
  }

  Result<std::size_t> WaitCompletionsFor(std::chrono::nanoseconds timeout) noexcept;

  void DrainStoppedOperations() noexcept;
  // Owner-loop resources that are not ring operations, such as sleeping
  // timers, register here so stopping can complete them before the drain.
  void RegisterShutdownParticipant(::alyrn::detail::LoopShutdownParticipant& participant) noexcept;
  void UnregisterShutdownParticipant(
      ::alyrn::detail::LoopShutdownParticipant& participant) noexcept;
  void BeginShutdown() noexcept;
  void HandleCqe(io_uring_cqe* cqe) noexcept;
  // Recycle provided buffers and resume CQE waiters before the next CQE so a
  // multishot recv can restock and rearm inside the same reap.
  void DrainCompletionReady() noexcept;
  void OnCqeHandled() noexcept;

  detail::Ring ring_;
  coro::WorkQueue ready_;
  coro::WorkQueue completion_ready_;
  bool initialized_{false};

  // Prepared SQEs that have not yet produced a CQE.
  std::size_t pending_submit_{0};

  // Submitted operations that have not yet produced a CQE.
  std::size_t inflight_{0};

  bool HasReadyWork() const noexcept { return !ready_.Empty() || !completion_ready_.Empty(); }

  Result<void> ArmWakePoll() noexcept;
  void DrainWakeFd() noexcept;
  void Wake() noexcept;

  // Cross-thread lifecycle state observed by the event loop.
  std::atomic<backend::LoopState> state_{backend::LoopState::kCreated};

  std::unique_ptr<detail::TimerQueue> timers_;
  ::alyrn::detail::LoopShutdownRegistry shutdown_registry_;
  bool shutdown_started_{false};
  int wake_fd_{-1};
  std::mutex post_mutex_;
  std::vector<std::function<void()>> posted_;
  bool post_closed_{false};
  bool wake_pending_{false};
  bool wake_inflight_{false};
  detail::Op wake_op_{detail::OpKind::kWake};
  bool cancel_all_pending_{false};
  detail::Op cancel_all_op_{detail::OpKind::kCancelAll};
  std::uint32_t next_buffer_group_id_{1};
  std::unique_ptr<detail::ProvidedBufferPool> shared_buffer_pool_;
  std::size_t shared_buffer_capacity_{0};
  std::size_t shared_buffer_size_{0};
  bool defer_task_run_{false};
};

static_assert(backend::ManagedLoop<Loop>);

}  // namespace alyrn::uring
