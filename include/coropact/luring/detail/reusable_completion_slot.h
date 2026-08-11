// SPDX-License-Identifier: MIT
#pragma once

#include <optional>

#include "coropact/operation/detail/completion_gate.h"

namespace coropact::luring::detail {

// Owns the physical settlement marker of one reusable io_uring operation slot.
//
// It rejects a duplicate terminal CQE and is reset only when the backend has
// released the prior physical request. Coupled stream awaiters keep their
// richer result/release/resume ordering in LUringOp's separate
// SingleResultLifecycle; composite, source, and split-release operations own
// their respective lifecycle state machines.
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
