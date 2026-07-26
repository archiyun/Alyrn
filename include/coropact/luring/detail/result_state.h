// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>

#include "coropact/base/error.h"

namespace coropact::luring::detail {

// Stream operations can complete before reaching io_uring, but their result is
// limited to success with zero bytes or an errno. Keep the pending/success/
// error state in one int instead of an optional expected. CloseAwaiter reuses
// the same encoding through the void specialization below.
class LUringResultStorage {
public:
  static constexpr std::int32_t kPending = std::numeric_limits<std::int32_t>::min();

  [[nodiscard]]
  bool IsImmediate() const noexcept {
    return encoded_ != kPending;
  }

  void SetSuccess() noexcept { encoded_ = 0; }

  void SetError(base::Error error) noexcept {
    assert(error.value() > 0 && error.value() != kPending);
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
  std::int32_t encoded() const noexcept {
    return encoded_;
  }

private:
  std::int32_t encoded_{kPending};
};

template <typename T>
class LUringResultState;

template <>
class LUringResultState<std::size_t> : private LUringResultStorage {
public:
  using LUringResultStorage::IsImmediate;
  using LUringResultStorage::SetError;
  using LUringResultStorage::SetResult;
  using LUringResultStorage::SetSuccess;

  [[nodiscard]]
  base::Result<std::size_t> Take() const noexcept {
    assert(IsImmediate());
    if (encoded() == 0) {
      return std::size_t{0};
    }
    return std::unexpected(base::make_errno(encoded()));
  }
};

template <>
class LUringResultState<void> : private LUringResultStorage {
public:
  using LUringResultStorage::IsImmediate;
  using LUringResultStorage::SetError;
  using LUringResultStorage::SetResult;
  using LUringResultStorage::SetSuccess;

  [[nodiscard]]
  base::Result<void> Take() const noexcept {
    assert(IsImmediate());
    if (encoded() == 0) {
      return {};
    }
    return std::unexpected(base::make_errno(encoded()));
  }
};

static_assert(sizeof(LUringResultState<std::size_t>) == 4);
static_assert(sizeof(LUringResultState<void>) == 4);

using LUringImmediateResult = LUringResultState<std::size_t>;

}  // namespace coropact::luring::detail
