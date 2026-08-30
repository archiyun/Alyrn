// SPDX-License-Identifier: MIT
#pragma once

#include <optional>

#include "alyrn/detail/completion_gate.h"

namespace alyrn::uring::detail {

// Owns the physical settlement marker of one reusable io_uring operation slot.
//
// It rejects a duplicate terminal CQE and is reset only when the backend has
// released the prior physical request. Coupled stream awaiters keep their
// richer result/release/resume ordering in Op's separate
// SingleResultLifecycle; composite, source, and split-release operations own
// their respective lifecycle state machines.
class ReusableCompletionSlot {
 public:
  bool TryComplete() noexcept {
    return completion_gate_->TryComplete();
  }

  bool Completed() const noexcept {
    return completion_gate_->Completed();
  }

  void BeginNextRequest() noexcept { completion_gate_.emplace(); }

private:
  std::optional<::alyrn::detail::CompletionGate> completion_gate_{std::in_place};
};

}  // namespace alyrn::uring::detail
