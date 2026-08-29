// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

#include "alyrn/result.h"
#include "alyrn/detail/uring/result_state.h"

namespace alyrn::uring::detail {

// The result uses 0 for success, a positive errno for failure, and INT_MIN
// for pending. The flags share this compact state object without widening the
// CloseAwaiter to the size of std::expected<void, Error>.
class CloseState {
public:
  void SetSuccess() noexcept { result_.SetSuccess(); }

  void SetError(Error error) noexcept { result_.SetError(error); }

  void SetResult(const Result<void>& result) noexcept { result_.SetResult(result); }

  bool HasResult() const noexcept {
    return result_.IsImmediate();
  }

  Result<void> TakeResult() const noexcept {
    return result_.Take();
  }

  bool CancelRequestTerminal() const noexcept {
    return (flags_ & kCancelRequestTerminalBit) != 0;
  }

  void MarkCancelRequestTerminal() noexcept { flags_ |= kCancelRequestTerminalBit; }

  bool Completed() const noexcept {
    return (flags_ & kCompletedBit) != 0;
  }

  void MarkCompleted() noexcept { flags_ |= kCompletedBit; }

private:
  static constexpr std::uint8_t kCancelRequestTerminalBit = 1U << 0;
  static constexpr std::uint8_t kCompletedBit = 1U << 1;

  ResultState<void> result_;
  std::uint8_t flags_{0};
};

static_assert(sizeof(CloseState) == 8);

}  // namespace alyrn::uring::detail
