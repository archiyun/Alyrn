// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <expected>
#include <limits>

#include "alyrn/detail/base/check.h"
#include "alyrn/result.h"

namespace alyrn::uring::detail {

// Stream operations can complete before reaching io_uring, but their result is
// limited to success with zero bytes or an errno. Keep the pending/success/
// error state in one int instead of an optional expected. CloseAwaiter reuses
// the same encoding through the void specialization below. This is terminal,
// awaiter-owned storage: Take() observes its one result but does not reopen it
// for reuse. Reusable physical Op slots use BeginNextRequest() instead.
class ResultStorage {
public:
  static constexpr std::int32_t kPending = std::numeric_limits<std::int32_t>::min();

  [[nodiscard]]
  bool IsImmediate() const noexcept {
    return encoded_ != kPending;
  }

  void SetSuccess() noexcept {
    ALYRN_CHECK(!IsImmediate(), "ResultStorage result was set twice");
    encoded_ = 0;
  }

  void SetError(Error error) noexcept {
    ALYRN_CHECK(!IsImmediate(), "ResultStorage result was set twice");
    ALYRN_CHECK(error.value() > 0 && error.value() != kPending,
                   "ResultStorage cannot encode this error");
    encoded_ = static_cast<std::int32_t>(error.value());
  }

  template <typename T>
  void SetResult(const Result<T>& result) noexcept {
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
class ResultState;

template <>
class ResultState<void> : private ResultStorage {
public:
  using ResultStorage::IsImmediate;
  using ResultStorage::SetError;
  using ResultStorage::SetResult;
  using ResultStorage::SetSuccess;

  [[nodiscard]]
  Result<void> Take() const noexcept {
    ALYRN_CHECK(IsImmediate(), "ResultState result was taken before completion");
    if (Encoded() == 0) {
      return {};
    }
    return std::unexpected(Errno(Encoded()));
  }
};

static_assert(sizeof(ResultState<void>) == 4);

}  // namespace alyrn::uring::detail
