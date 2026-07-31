// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/op.h"

namespace coropact::luring::detail {

// Bridges bool await_suspend() to LUringLoop::SubmitOp.
//
// The caller owns resource-specific state, including validation, pending-slot
// reservation, SQE preparation, and rollback. This helper only binds the
// continuation and transfers completion responsibility to the loop:
//`
//   bind continuation -> submit -> clear continuation and rollback on failure
//
// On failure, await_suspend() returns false, so the coroutine continues inline.
// The continuation must therefore be cleared before invoking rollback.
template <typename Prep, typename OnSubmitFailure>
[[nodiscard]]
bool SubmitAwaitingOperation(LUringLoop& loop, LUringOp& op, std::coroutine_handle<> continuation,
                             Prep&& prep, OnSubmitFailure&& on_submit_failure) noexcept {
  COROPACT_CHECK(loop.IsInLoopThread(), "LUring operation submitted from a non-owner loop thread");

  op.resume_work.SetHandle(continuation);
  auto submitted = loop.SubmitOp(&op, std::forward<Prep>(prep));
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
