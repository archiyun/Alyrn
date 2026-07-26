// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "coropact/base/error.h"
#include "coropact/coro/work.h"

namespace coropact::luring {

enum class LUringOpKind : std::uint8_t {
  kNone = 0,

  kAcceptComplete,
  kListenerCloseComplete,

  kReadComplete,
  kTimedReadComplete,
  kTimedReadTimeoutComplete,

  kWriteComplete,
  kWritePartsComplete,
  kStreamCloseComplete,

  kTimerDriverComplete,
  kTimerControlComplete,

  kConnect,
  kMsgRing,
  kWake,
  kCancelAll,
  kNop,

  // Count/placeholder sentinel. It is not a submit-able operation kind.
  kCount,
};

// A CQE result is always an integer. Kernel errors are represented by a
// negative cqe_res and converted to base::Error by the awaiter, so storing a
// full std::expected<int, Error> here needlessly adds the error union and its
// alignment padding to every operation. This compact representation relies on
// the Linux io_uring contract that cqe_res contains a non-INT_MIN result (a
// non-negative return value or a negative -errno). If a backend can return an
// arbitrary int, replace the sentinel encoding with a wider/tagged form.
class LUringCqeResult {
public:
  constexpr LUringCqeResult() noexcept = default;

  LUringCqeResult& operator=(int value) noexcept {
    assert(value != kEmpty && "INT_MIN is reserved for an empty CQE result");
    encoded_ = static_cast<std::int32_t>(value);
    return *this;
  }

  [[nodiscard]]
  bool has_value() const noexcept {
    return encoded_ != kEmpty;
  }

  [[nodiscard]]
  int operator*() const noexcept {
    assert(has_value());
    return static_cast<int>(encoded_);
  }

  // There is no stored error object: CQE failures are the negative integer
  // itself and are converted by the awaiter. Calling error() for an empty
  // result is only a defensive fallback for the invalid pre-completion path.
  [[nodiscard]]
  base::Error error() const noexcept {
    assert(!has_value());
    return base::make_errno(EIO);
  }

private:
  static constexpr std::int32_t kEmpty = std::numeric_limits<std::int32_t>::min();
  std::int32_t encoded_{kEmpty};
};

static_assert(sizeof(int) == sizeof(std::int32_t));
static_assert(sizeof(LUringCqeResult) == 4);

class LUringOp {
public:
  // The top bit of kind is reserved for completion state. Add operation kinds
  // below 0x80 and use IsCompleted()/ResetCompletion() instead of treating
  // the raw kind byte as an ordinary enum after completion.
  coro::ResumeWork resume_work;
  LUringCqeResult result;
  LUringOpKind kind{};

  [[nodiscard]]
  bool Complete(int cqe_res) noexcept {
    if (IsCompleted()) {
      return false;
    }

    result = cqe_res;
    MarkCompleted();
    return true;
  }

  void SetImmediateSuccess() noexcept { result = 0; }

  void SetImmediateError(base::Error error) noexcept {
    assert(error.value() > 0);
    result = -error.value();
  }

  [[nodiscard]]
  LUringOpKind DispatchKind() const noexcept {
    return static_cast<LUringOpKind>(static_cast<std::uint8_t>(kind) & kDispatchMask);
  }

  [[nodiscard]]
  bool IsCompleted() const noexcept {
    return (static_cast<std::uint8_t>(kind) & kCompletedBit) != 0;
  }

  void ResetCompletion() noexcept {
    kind = static_cast<LUringOpKind>(static_cast<std::uint8_t>(kind) & ~kCompletedBit);
  }

private:
  static constexpr std::uint8_t kCompletedBit = 0x80;
  static constexpr std::uint8_t kDispatchMask = 0x7f;
  static_assert(static_cast<std::uint8_t>(LUringOpKind::kCount) < kCompletedBit);

  void MarkCompleted() noexcept {
    kind = static_cast<LUringOpKind>(static_cast<std::uint8_t>(kind) | kCompletedBit);
  }
};

static_assert(sizeof(LUringOp) == 24);

}  // namespace coropact::luring
