// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/detail/operation/completion_gate.h"

namespace alyrn::detail::operation {

/*
 * Lifecycle protocol for operations whose logical result and resource release
 * occur at different physical completion boundaries.
 *
 * A backend records the caller-visible result, marks the physical request
 * terminal once it cannot access resources, and then authorizes release and
 * continuation in order. The object is thread-confined and owns no result,
 * buffer, fd, CQE, or continuation.
 */
class SplitReleaseLifecycle {
public:
  SplitReleaseLifecycle() noexcept = default;
  SplitReleaseLifecycle(const SplitReleaseLifecycle&) = delete;
  SplitReleaseLifecycle& operator=(const SplitReleaseLifecycle&) = delete;
  SplitReleaseLifecycle(SplitReleaseLifecycle&&) = delete;
  SplitReleaseLifecycle& operator=(SplitReleaseLifecycle&&) = delete;

  bool RecordLogicalResult() noexcept {
    return logical_result_.TryComplete();
  }

  bool MarkPhysicalTerminal() noexcept {
    if (physical_terminal_) {
      return false;
    }
    physical_terminal_ = true;
    return true;
  }

  bool TryAuthorizeRelease() noexcept {
    if (!logical_result_.Completed() || !physical_terminal_) {
      return false;
    }
    return release_authorized_.TryComplete();
  }

  bool TryAuthorizeContinuation() noexcept {
    if (!release_authorized_.Completed()) {
      return false;
    }
    return continuation_authorized_.TryComplete();
  }

  bool LogicalResultReady() const noexcept { return logical_result_.Completed(); }
  bool PhysicalTerminal() const noexcept { return physical_terminal_; }
  bool ReleaseAuthorized() const noexcept { return release_authorized_.Completed(); }
  bool ContinuationAuthorized() const noexcept {
    return continuation_authorized_.Completed();
  }

private:
  CompletionGate logical_result_;
  CompletionGate release_authorized_;
  CompletionGate continuation_authorized_;
  bool physical_terminal_{false};
};

static_assert(sizeof(SplitReleaseLifecycle) == 4);

}  // namespace alyrn::detail::operation
