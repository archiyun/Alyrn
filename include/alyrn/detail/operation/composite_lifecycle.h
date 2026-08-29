// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

#include "alyrn/detail/operation/completion_gate.h"

namespace alyrn::detail::operation {

enum class CompositeMember : std::uint8_t {
  kFirst,
  kSecond,
};

/*
 * Lifecycle protocol for one logical result derived from two independently
 * completing physical members, such as a linked read and timeout.
 *
 * Operation-specific handlers retain member results and decide their meaning.
 * This core records each member once, then separately authorizes the logical
 * result, release, and one continuation resumption. Release remains explicit
 * even for a coupled composite.
 */
class CompositeLifecycle {
public:
  CompositeLifecycle() noexcept = default;
  CompositeLifecycle(const CompositeLifecycle&) = delete;
  CompositeLifecycle& operator=(const CompositeLifecycle&) = delete;
  CompositeLifecycle(CompositeLifecycle&&) = delete;
  CompositeLifecycle& operator=(CompositeLifecycle&&) = delete;

  bool RecordMemberCompletion(CompositeMember member) noexcept {
    return GateFor(member).TryComplete();
  }

  bool MemberCompleted(CompositeMember member) const noexcept {
    return GateFor(member).Completed();
  }

  bool AllMembersCompleted() const noexcept {
    return first_member_.Completed() && second_member_.Completed();
  }

  bool TryAuthorizeLogicalResult() noexcept {
    if (!AllMembersCompleted()) {
      return false;
    }
    return logical_result_.TryComplete();
  }

  bool TryAuthorizeRelease() noexcept {
    if (!logical_result_.Completed()) {
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

  bool LogicalResultAuthorized() const noexcept {
    return logical_result_.Completed();
  }
  bool ReleaseAuthorized() const noexcept { return release_authorized_.Completed(); }
  bool ContinuationAuthorized() const noexcept {
    return continuation_authorized_.Completed();
  }

private:
  CompletionGate& GateFor(CompositeMember member) noexcept {
    return member == CompositeMember::kFirst ? first_member_ : second_member_;
  }

  const CompletionGate& GateFor(CompositeMember member) const noexcept {
    return member == CompositeMember::kFirst ? first_member_ : second_member_;
  }

  CompletionGate first_member_;
  CompletionGate second_member_;
  CompletionGate logical_result_;
  CompletionGate release_authorized_;
  CompletionGate continuation_authorized_;
};

static_assert(sizeof(CompositeLifecycle) == 5);

}  // namespace alyrn::detail::operation
