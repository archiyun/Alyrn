// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include "coropact/luring/detail/op.h"
#include "coropact/luring/loop.h"
#include "coropact/operation/detail/scheduler_continuation.h"

namespace coropact::luring::detail {

// Internal access point for luring adapters. It keeps SQE/CQE bookkeeping,
// mailbox transport, and manual completion driving out of LUringLoop's public
// interface while allowing the backend implementation to retain one owner.
class LoopAccess final {
public:
  template <class Prep>
  [[nodiscard]]
  static base::Result<void> SubmitOp(LUringLoop& loop, LUringOp* op, Prep&& prep) noexcept {
    return loop.SubmitOp(op, std::forward<Prep>(prep));
  }

  [[nodiscard]]
  static base::Result<void> SubmitMsgRing(LUringLoop& loop, LUringOp* op, int target_ring_fd,
                                          std::uint32_t type) noexcept {
    return loop.SubmitMsgRing(op, target_ring_fd, type);
  }

  static void ScheduleCompletion(LUringLoop& loop, coro::Work* work) noexcept {
    loop.ScheduleCompletion(work);
  }

  // Preserves a continuation's captured scheduler affinity while using the
  // loop's bounded-priority completion queue for work produced by a CQE.
  static void ScheduleCompletion(LUringLoop& loop,
                                 operation::detail::SchedulerContinuation& continuation) noexcept {
    continuation.ScheduleWith([&loop](coro::Scheduler& scheduler, coro::Work* work) noexcept {
      COROPACT_CHECK(&scheduler == static_cast<coro::Scheduler*>(&loop),
                     "luring continuation is bound to a different scheduler");
      loop.ScheduleCompletion(work);
    });
  }

  [[nodiscard]]
  static LUringMailboxPushResult PostMessage(LUringLoop& loop, LUringMessage message) {
    return loop.PostMessage(message);
  }

  [[nodiscard]]
  static bool RetryMessageNotification(LUringLoop& loop) noexcept {
    return loop.RetryMessageNotification();
  }

  [[nodiscard]]
  static base::Result<void> FlushSubmit(LUringLoop& loop) noexcept {
    return loop.FlushSubmit();
  }

  [[nodiscard]]
  static base::Result<void> CancelPendingOperations(LUringLoop& loop) noexcept {
    return loop.CancelPendingOperations();
  }

  [[nodiscard]]
  static base::Result<std::size_t> PollCompletions(LUringLoop& loop) noexcept {
    return loop.PollCompletions();
  }

  [[nodiscard]]
  static base::Result<std::size_t> WaitCompletions(LUringLoop& loop) noexcept {
    return loop.WaitCompletions();
  }

  static void RunReady(LUringLoop& loop) noexcept { loop.RunReady(); }

  [[nodiscard]]
  static int RingFd(const LUringLoop& loop) noexcept {
    return loop.RingFd();
  }

  [[nodiscard]]
  static std::size_t PendingSubmitCount(const LUringLoop& loop) noexcept {
    return loop.PendingSubmitCount();
  }

  [[nodiscard]]
  static std::size_t InflightCount(const LUringLoop& loop) noexcept {
    return loop.InflightCount();
  }

  [[nodiscard]]
  static bool IsDrained(const LUringLoop& loop) noexcept {
    return loop.IsDrained();
  }

  [[nodiscard]]
  static base::Result<ProvidedBufferPool*> GetSharedProvidedBufferPool(
      LUringLoop& loop, std::size_t buffer_size, std::size_t source_capacity) noexcept {
    return loop.GetSharedProvidedBufferPool(buffer_size, source_capacity);
  }
};

}  // namespace coropact::luring::detail
