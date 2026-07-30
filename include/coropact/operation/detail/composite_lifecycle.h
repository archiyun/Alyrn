// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/operation_family.h"

namespace coropact::operation::detail {

enum class CompositeMember : std::uint8_t {
  kFirst,
  kSecond,
};

// Lifecycle protocol for an operation whose one logical result depends on two
// independently completing physical members, such as a linked read+timeout.
//
// Family handlers retain the member results and decide their business meaning.
// This core only records each physical member once and authorizes one logical
// completion and one continuation resume after both are observed.
class CompositeLifecycle {
public:
  CompositeLifecycle() noexcept = default;
  CompositeLifecycle(const CompositeLifecycle&) = delete;
  CompositeLifecycle& operator=(const CompositeLifecycle&) = delete;
  CompositeLifecycle(CompositeLifecycle&&) = delete;
  CompositeLifecycle& operator=(CompositeLifecycle&&) = delete;

  [[nodiscard]]
  static constexpr OperationFamily Family() noexcept {
    return OperationFamily::kComposite;
  }

  [[nodiscard]]
  bool RecordMemberCompletion(CompositeMember member) noexcept {
    return GateFor(member).TryComplete();
  }

  [[nodiscard]]
  bool MemberCompleted(CompositeMember member) const noexcept {
    return GateFor(member).Completed();
  }

  [[nodiscard]]
  bool AllMembersCompleted() const noexcept {
    return first_member_.Completed() && second_member_.Completed();
  }

  [[nodiscard]]
  bool TryAuthorizeLogicalResult() noexcept {
    if (!AllMembersCompleted()) {
      return false;
    }
    return logical_result_.TryComplete();
  }

  [[nodiscard]]
  bool TryAuthorizeContinuation() noexcept {
    if (!logical_result_.Completed()) {
      return false;
    }
    return continuation_authorized_.TryComplete();
  }

  [[nodiscard]] bool LogicalResultAuthorized() const noexcept {
    return logical_result_.Completed();
  }
  [[nodiscard]] bool ContinuationAuthorized() const noexcept {
    return continuation_authorized_.Completed();
  }

private:
  [[nodiscard]] CompletionGate& GateFor(CompositeMember member) noexcept {
    return member == CompositeMember::kFirst ? first_member_ : second_member_;
  }

  [[nodiscard]] const CompletionGate& GateFor(CompositeMember member) const noexcept {
    return member == CompositeMember::kFirst ? first_member_ : second_member_;
  }

  CompletionGate first_member_;
  CompletionGate second_member_;
  CompletionGate logical_result_;
  CompletionGate continuation_authorized_;
};

static_assert(sizeof(CompositeLifecycle) == 4);

}  // namespace coropact::operation::detail
