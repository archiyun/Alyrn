// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

#include "coropact/result.h"
#include "coropact/luring/detail/result_state.h"

namespace coropact::luring::detail {

// The result uses 0 for success, a positive errno for failure, and INT_MIN
// for pending. The flags share this compact state object without widening the
// CloseAwaiter to the size of std::expected<void, Error>.
class LUringCloseState {
public:
  void SetSuccess() noexcept { result_.SetSuccess(); }

  void SetError(Error error) noexcept { result_.SetError(error); }

  void SetResult(const Result<void>& result) noexcept { result_.SetResult(result); }

  [[nodiscard]]
  bool HasResult() const noexcept {
    return result_.IsImmediate();
  }

  [[nodiscard]]
  Result<void> TakeResult() const noexcept {
    return result_.Take();
  }

  [[nodiscard]]
  bool CancelRequestTerminal() const noexcept {
    return (flags_ & kCancelRequestTerminalBit) != 0;
  }

  void MarkCancelRequestTerminal() noexcept { flags_ |= kCancelRequestTerminalBit; }

  [[nodiscard]]
  bool Completed() const noexcept {
    return (flags_ & kCompletedBit) != 0;
  }

  void MarkCompleted() noexcept { flags_ |= kCompletedBit; }

private:
  static constexpr std::uint8_t kCancelRequestTerminalBit = 1U << 0;
  static constexpr std::uint8_t kCompletedBit = 1U << 1;

  LUringResultState<void> result_;
  std::uint8_t flags_{0};
};

static_assert(sizeof(LUringCloseState) == 8);

}  // namespace coropact::luring::detail
