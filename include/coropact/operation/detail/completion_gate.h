// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/utils/macros.h"

namespace coropact::operation::detail {

// Owns the logical terminal transition for one single-result operation.
//
// The gate is intentionally thread-confined. Backends marshal physical
// completion, cancellation, and close events onto the operation owner before
// calling TryComplete(). It does not own a result, continuation, fd, or
// buffer, so the core has no I/O dependency.
//
// Multi-shot and split-release families decide when a physical event is
// terminal through their own policy, then use this gate for a single logical
// completion when appropriate.
class CompletionGate {
public:
  COROPACT_DELETE_COPY_MOVE(CompletionGate);

  CompletionGate() noexcept = default;

  [[nodiscard]]
  bool TryComplete() noexcept {
    if (completed_) {
      return false;
    }
    completed_ = true;
    return true;
  }

  [[nodiscard]]
  bool Completed() const noexcept {
    return completed_;
  }

private:
  bool completed_{false};
};

static_assert(sizeof(CompletionGate) == 1);

}  // namespace coropact::operation::detail
