// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include "coropact/luring/detail/op.h"
#include "coropact/luring/detail/provided_buffer_pool.h"
#include "coropact/luring/loop.h"
#include "coropact/operation/detail/scheduler_continuation.h"

namespace coropact::luring::detail {

// Internal access point for luring adapters. It keeps SQE/CQE bookkeeping,
// manual completion driving out of Loop's public interface while allowing the
// backend implementation to retain one owner.
class LoopAccess final {
public:
  template <class Prep>
  [[nodiscard]]
  static Result<void> SubmitOp(Loop& loop, Op* op, Prep&& prep) noexcept {
    return loop.SubmitOp(op, std::forward<Prep>(prep));
  }

  static void ScheduleCompletion(Loop& loop, coro::Work* work) noexcept {
    loop.ScheduleCompletion(work);
  }

  // Preserves a continuation's captured scheduler affinity while using the
  // loop's bounded-priority completion queue for work produced by a CQE.
  static void ScheduleCompletion(Loop& loop,
                                 operation::detail::SchedulerContinuation& continuation) noexcept {
    continuation.ScheduleWith([&loop](coro::Scheduler& scheduler, coro::Work* work) noexcept {
      COROPACT_CHECK(&scheduler == static_cast<coro::Scheduler*>(&loop),
                     "luring continuation is bound to a different scheduler");
      loop.ScheduleCompletion(work);
    });
  }

  [[nodiscard]]
  static Result<void> FlushSubmit(Loop& loop) noexcept {
    return loop.FlushSubmit();
  }

  [[nodiscard]]
  static Result<void> CancelPendingOperations(Loop& loop) noexcept {
    return loop.CancelPendingOperations();
  }

  [[nodiscard]]
  static Result<std::size_t> PollCompletions(Loop& loop) noexcept {
    return loop.PollCompletions();
  }

  [[nodiscard]]
  static Result<std::size_t> WaitCompletions(Loop& loop) noexcept {
    return loop.WaitCompletions();
  }

  static void RunReady(Loop& loop) noexcept { loop.RunReady(); }

  [[nodiscard]]
  static int RingFd(const Loop& loop) noexcept {
    return loop.RingFd();
  }

  [[nodiscard]]
  static std::size_t PendingSubmitCount(const Loop& loop) noexcept {
    return loop.PendingSubmitCount();
  }

  [[nodiscard]]
  static std::size_t InflightCount(const Loop& loop) noexcept {
    return loop.InflightCount();
  }

  [[nodiscard]]
  static bool IsDrained(const Loop& loop) noexcept {
    return loop.IsDrained();
  }

  [[nodiscard]]
  static Result<ProvidedBufferPool*> GetSharedProvidedBufferPool(
      Loop& loop, std::size_t buffer_size, std::size_t source_capacity) noexcept {
    return loop.GetSharedProvidedBufferPool(buffer_size, source_capacity);
  }
};

}  // namespace coropact::luring::detail
