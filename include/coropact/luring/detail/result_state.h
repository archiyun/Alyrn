// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <expected>
#include <limits>

#include "coropact/base/check.h"
#include "coropact/base/error.h"

namespace coropact::luring::detail {

// Stream operations can complete before reaching io_uring, but their result is
// limited to success with zero bytes or an errno. Keep the pending/success/
// error state in one int instead of an optional expected. CloseAwaiter reuses
// the same encoding through the void specialization below. This is terminal,
// awaiter-owned storage: Take() observes its one result but does not reopen it
// for reuse. Reusable physical LUringOp slots use BeginNextRequest() instead.
class LUringResultStorage {
public:
  static constexpr std::int32_t kPending = std::numeric_limits<std::int32_t>::min();

  [[nodiscard]]
  bool IsImmediate() const noexcept {
    return encoded_ != kPending;
  }

  void SetSuccess() noexcept {
    COROPACT_CHECK(!IsImmediate(), "LUringResultStorage result was set twice");
    encoded_ = 0;
  }

  void SetError(base::Error error) noexcept {
    COROPACT_CHECK(!IsImmediate(), "LUringResultStorage result was set twice");
    COROPACT_CHECK(error.value() > 0 && error.value() != kPending,
                   "LUringResultStorage cannot encode this error");
    encoded_ = static_cast<std::int32_t>(error.value());
  }

  template <typename T>
  void SetResult(const base::Result<T>& result) noexcept {
    if (result.has_value()) {
      SetSuccess();
    } else {
      SetError(result.error());
    }
  }

protected:
  [[nodiscard]]
  std::int32_t Encoded() const noexcept {
    return encoded_;
  }

private:
  std::int32_t encoded_{kPending};
};

template <typename T>
class LUringResultState;

template <>
class LUringResultState<void> : private LUringResultStorage {
public:
  using LUringResultStorage::IsImmediate;
  using LUringResultStorage::SetError;
  using LUringResultStorage::SetResult;
  using LUringResultStorage::SetSuccess;

  [[nodiscard]]
  base::Result<void> Take() const noexcept {
    COROPACT_CHECK(IsImmediate(), "LUringResultState result was taken before completion");
    if (Encoded() == 0) {
      return {};
    }
    return std::unexpected(base::MakeErrno(Encoded()));
  }
};

static_assert(sizeof(LUringResultState<void>) == 4);

}  // namespace coropact::luring::detail
