// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <utility>

#include "alyrn/detail/check.h"
#include "alyrn/uring/detail/loop_access.h"
#include "alyrn/uring/loop.h"

namespace alyrn::uring::detail {

// Submits one single-result awaiter operation without adding state to Op.
//
// The caller owns all resource-specific state: validation, pending-slot
// reservation, SQE preparation, and rollback after a submission failure. This
// adapter owns only the common transition from a suspended coroutine to a
// queued physical request:
//
//   bind ResumeWork -> SubmitOp -> clear ResumeWork and rollback on failure
//
// It is deliberately uring-specific. Completion-protocol interpretation stays
// with the submitting awaiter, while detail/operation remains resource-free.
template <typename Prep, typename OnSubmitFailure>
bool SubmitAwaitingOperation(Loop& loop, Op& op, std::coroutine_handle<> continuation,
                             Prep&& prep, OnSubmitFailure&& on_submit_failure) noexcept {
  ALYRN_CHECK(loop.IsInLoopThread(), "Uring operation submitted from a non-owner loop thread");

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

}  // namespace alyrn::uring::detail
