// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>

#include "alyrn/result.h"
#include "alyrn/detail/uring/close_state.h"

namespace alyrn::uring::detail {

// Coordinates a close that must wait for two independent facts: the cancel
// request completed and every operation that could still reference the fd has
// drained. The owner performs the actual ::close(), because it owns the fd.
class FdCloseConvergence {
public:
  void SetSuccess() noexcept { state_.SetSuccess(); }

  void SetError(Error error) noexcept { state_.SetError(error); }

  void SetResult(const Result<void>& result) noexcept { state_.SetResult(result); }

  [[nodiscard]]
  bool HasResult() const noexcept {
    return state_.HasResult();
  }

  [[nodiscard]]
  Result<void> TakeResult() const noexcept {
    return state_.TakeResult();
  }

  void BeginWaiting(std::coroutine_handle<> continuation) noexcept { continuation_ = continuation; }

  void MarkCancelRequestTerminal() noexcept { state_.MarkCancelRequestTerminal(); }

  // Returns true exactly once, after the cancel request itself and every
  // physical operation using the descriptor have reached terminal state. The
  // caller may then close the descriptor and resume Continuation().
  [[nodiscard]]
  bool TryAuthorizeClose(bool operations_pending) noexcept {
    if (state_.Completed() || !state_.CancelRequestTerminal() || operations_pending) {
      return false;
    }
    state_.MarkCompleted();
    return true;
  }

  [[nodiscard]]
  std::coroutine_handle<> Continuation() const noexcept {
    return continuation_;
  }

private:
  CloseState state_;
  std::coroutine_handle<> continuation_{};
};

static_assert(sizeof(FdCloseConvergence) == 16);

}  // namespace alyrn::uring::detail
