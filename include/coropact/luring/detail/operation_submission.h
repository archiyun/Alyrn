// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/luring/detail/loop_access.h"
#include "coropact/luring/loop.h"

namespace coropact::luring::detail {

// Submits one single-result awaiter operation without adding state to LUringOp.
//
// The caller owns all resource-specific state: validation, pending-slot
// reservation, SQE preparation, and rollback after a submission failure. This
// adapter owns only the common transition from a suspended coroutine to a
// queued physical request:
//
//   bind ResumeWork -> SubmitOp -> clear ResumeWork and rollback on failure
//
// It is deliberately luring-specific. Completion-protocol interpretation stays
// with the submitting awaiter, while operation/detail remains resource-free.
template <typename Prep, typename OnSubmitFailure>
[[nodiscard]]
bool SubmitAwaitingOperation(LUringLoop& loop, LUringOp& op, std::coroutine_handle<> continuation,
                             Prep&& prep, OnSubmitFailure&& on_submit_failure) noexcept {
  COROPACT_CHECK(loop.IsInLoopThread(), "LUring operation submitted from a non-owner loop thread");

  op.resume_work.SetHandle(continuation);
  auto submitted = LoopAccess::SubmitOp(loop, &op, std::forward<Prep>(prep));
  if (submitted.has_value()) {
    return true;
  }

  // await_suspend() returns false after rollback, so retaining a handle here
  // would permit a stale CQE or a later accidental schedule to resume a frame
  // which has already continued inline.
  op.resume_work.ClearHandle();
  std::forward<OnSubmitFailure>(on_submit_failure)(submitted.error());
  return false;
}

}  // namespace coropact::luring::detail
