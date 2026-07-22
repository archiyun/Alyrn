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
#include <stop_token>
#include <utility>

#include "vexo/base/current_thread.h"
#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/work.h"
#include "vexo/luring/mailbox.h"
#include "vexo/luring/op.h"
#include "vexo/luring/options.h"
#include "vexo/luring/ring.h"
#include "vexo/luring/timer_queue.h"
#include "vexo/time/timer_id.h"

namespace vexo::luring {

// Single-threaded io_uring event loop
//
// Each LUringLoop owns one LUringRing and is bound to the thread that creates
// it. IO operations are submitted to the ring, and completed operations resume
// their coroutine work through the Scheduler interface.
//
// Notify function:
//   target.PostMessage() -> source.Notify() -> target.HandleCqe() -> target.Schedule(work)
class LUringLoop final : public coro::Scheduler {
public:
  VEXO_DELETE_COPY_MOVE(LUringLoop);

  // frame_resource is used for coroutine frames Scheduled by this loop.
  explicit LUringLoop(std::pmr::memory_resource* frame_resource = nullptr);

  // Initializes the underlying io_uring instance.
  // Must be called from the loop thread before Loop().
  [[nodiscard]] base::Result<void> Init(const LUringOptions& options) noexcept;

  [[nodiscard]] bool initialized() const noexcept { return initialized_; }

  // Runs the event loop until cancellation or Quit().
  void Loop(std::stop_token token) noexcept;

  // Requests the event loop to exit.
  // This function may be called from another thread.
  void Quit() noexcept;

  [[nodiscard]] bool IsInLoopThread() const noexcept { return thread_id_ == base::tid(); }
  [[nodiscard]] int thread_id() const noexcept { return thread_id_; }

  [[nodiscard]] int ring_fd() const noexcept { return ring_.fd(); }

  [[nodiscard]] std::size_t PendingSubmitCount() const noexcept { return pending_submit_; }

  [[nodiscard]] std::size_t InflightCount() const noexcept { return inflight_; }

  [[nodiscard]] bool IsDrained() const noexcept { return pending_submit_ == 0 && inflight_ == 0; }

  [[nodiscard]] base::Result<time::TimerId> RunAfter(std::chrono::steady_clock::duration delay,
                                                     LUringTimerQueue::TimerCallback callback) {
    assert(IsInLoopThread());
    if (!initialized_) {
      return std::unexpected(base::make_errno(EBADF));
    }
    return timers_.AddAfter(delay, std::move(callback));
  }

  base::Result<void> CancelTimer(time::TimerId id) noexcept {
    assert(IsInLoopThread());
    if (!initialized_) {
      return std::unexpected(base::make_errno(EBADF));
    }
    return timers_.Cancel(id);
  }

  // Enqueues coroutine work to be resumed by RunReady().
  void Schedule(coro::Work* work) noexcept override;

  // Thread-safe enqueue
  // The event loop is not woken yet; msg_ring will provide notification later.
  [[nodiscard]] LUringMailboxPushResult PostMessage(LUringMessage message) {
    return mailbox_.Push(std::move(message));
  }

  // Re-arms notification after a source-side msg_ring submission failure.
  [[nodiscard]] bool RetryMessageNotification() noexcept { return mailbox_.RetryNotification(); }

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
  [[nodiscard]] base::Result<void> SubmitOp(LUringOp* op, Prep&& prep) noexcept {
    assert(IsInLoopThread());

    if (!initialized_) {
      return std::unexpected(base::make_errno(EBADF));
    }
    if (op == nullptr) {
      return std::unexpected(base::make_errno(EINVAL));
    }

    io_uring_sqe* sqe = ring_.GetSqe();
    if (sqe == nullptr) {
      auto flushed = FlushSubmit();
      if (!flushed.has_value()) {
        return std::unexpected(flushed.error());
      }

      sqe = ring_.GetSqe();
      if (sqe == nullptr) {
        return std::unexpected(base::make_errno(ENOSPC));
      }
    }

    prep(sqe);
    io_uring_sqe_set_data(sqe, op);
    ++pending_submit_;
    return {};
  }

  [[nodiscard]] base::Result<void> SubmitMsgRing(LUringOp* op, int target_ring_fd,
                                                 std::uint32_t type) noexcept {
    assert(IsInLoopThread());

    if (target_ring_fd < 0) {
      return std::unexpected(base::make_errno(EBADF));
    }

    return SubmitOp(op, [this, target_ring_fd, type](io_uring_sqe* sqe) noexcept {
      ring_.PrepMsgRing(sqe, target_ring_fd, type, kMsgRingNotificationUserData);
    });
  }

  [[nodiscard]] base::Result<void> Notify(LUringLoop& target, LUringOp* op) noexcept {
    assert(IsInLoopThread());

    if (op == nullptr) {
      return std::unexpected(base::make_errno(EINVAL));
    }

    return SubmitMsgRing(op, target.ring_fd(), 0);
  }

  [[nodiscard]] base::Result<void> FlushSubmit() noexcept;
  [[nodiscard]] base::Result<std::size_t> PollCompletions() noexcept;
  [[nodiscard]] base::Result<std::size_t> WaitCompletions() noexcept;

  void RunReady() noexcept;
  void RunUntilIdle();

private:
  [[nodiscard]] base::Result<std::size_t> WaitCompletionsFor(
      std::chrono::nanoseconds timeout) noexcept;

  void HandleCqe(io_uring_cqe* cqe) noexcept;
  void HandleMailbox() noexcept;

  const int thread_id_;
  LUringRing ring_;
  coro::WorkQueue ready_;
  bool initialized_{false};

  // Prepared SQEs that have not yet produced a CQE.
  std::size_t pending_submit_{0};

  // Submitted operations that have not yet produced a CQE.
  std::size_t inflight_{0};

  // Preferred number of prepared operations before performing a batch submit.
  std::size_t submit_batch_{32};

  // Fairness budget for one RunReady() pass. Zero means unlimited.
  std::size_t max_ready_work_per_turn_{256};

  // Cross-thread exit request observed by the event loop.
  std::atomic_bool quit_{false};

  LUringMailbox mailbox_;
  LUringTimerQueue timers_;
};

}  // namespace vexo::luring
