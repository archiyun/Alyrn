// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/operation/detail/completion_gate.h"

namespace coropact::operation::detail {

// Lifecycle protocol for an operation whose logical result and resource
// release occur at different physical completion boundaries.
//
// The backend handler supplies the interpretation of backend events. It calls:
//   1. RecordLogicalResult() when the caller-visible result is known;
//   2. MarkPhysicalTerminal() when the backend can no longer access resources;
//   3. TryAuthorizeRelease() to release those resources exactly once; and
//   4. TryAuthorizeContinuation() to resume the awaiting coroutine exactly
//      once, after release has been authorized.
//
// The object is thread-confined and owns no result, buffer, fd, CQE, or
// continuation. It is therefore shared by backend adapters without imposing
// an I/O abstraction on them.
class SplitReleaseLifecycle {
public:
  SplitReleaseLifecycle() noexcept = default;
  SplitReleaseLifecycle(const SplitReleaseLifecycle&) = delete;
  SplitReleaseLifecycle& operator=(const SplitReleaseLifecycle&) = delete;
  SplitReleaseLifecycle(SplitReleaseLifecycle&&) = delete;
  SplitReleaseLifecycle& operator=(SplitReleaseLifecycle&&) = delete;

  [[nodiscard]]
  bool RecordLogicalResult() noexcept {
    return logical_result_.TryComplete();
  }

  [[nodiscard]]
  bool MarkPhysicalTerminal() noexcept {
    if (physical_terminal_) {
      return false;
    }
    physical_terminal_ = true;
    return true;
  }

  [[nodiscard]]
  bool TryAuthorizeRelease() noexcept {
    if (!logical_result_.Completed() || !physical_terminal_) {
      return false;
    }
    return release_authorized_.TryComplete();
  }

  [[nodiscard]]
  bool TryAuthorizeContinuation() noexcept {
    if (!release_authorized_.Completed()) {
      return false;
    }
    return continuation_authorized_.TryComplete();
  }

  [[nodiscard]] bool LogicalResultReady() const noexcept { return logical_result_.Completed(); }
  [[nodiscard]] bool PhysicalTerminal() const noexcept { return physical_terminal_; }
  [[nodiscard]] bool ReleaseAuthorized() const noexcept { return release_authorized_.Completed(); }
  [[nodiscard]] bool ContinuationAuthorized() const noexcept {
    return continuation_authorized_.Completed();
  }

private:
  CompletionGate logical_result_;
  CompletionGate release_authorized_;
  CompletionGate continuation_authorized_;
  bool physical_terminal_{false};
};

static_assert(sizeof(SplitReleaseLifecycle) == 4);

}  // namespace coropact::operation::detail
