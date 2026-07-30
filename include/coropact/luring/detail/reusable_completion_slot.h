// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <optional>

#include "coropact/operation/detail/completion_gate.h"

namespace coropact::luring::detail {

// Owns the completion state of one reusable io_uring operation slot.
//
// CompletionGate itself models one logical result and is never reopened. A
// physical LUringOp is reused only after its prior request has reached its
// backend-defined release point; BeginNextRequest() then installs a fresh
// logical gate for the next request.
class ReusableCompletionSlot {
 public:
  [[nodiscard]]
  bool TryComplete() noexcept {
    return completion_gate_->TryComplete();
  }

  [[nodiscard]]
  bool Completed() const noexcept {
    return completion_gate_->Completed();
  }

  void BeginNextRequest() noexcept { completion_gate_.emplace(); }

private:
  std::optional<operation::detail::CompletionGate> completion_gate_{std::in_place};
};

}  // namespace coropact::luring::detail
