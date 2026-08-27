// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <system_error>
#include <utility>

#include "alyrn/detail/base/check.h"
#include "alyrn/result.h"

namespace alyrn::kqueue::detail {

// Read/write attempts only need a non-negative byte count or a positive system
// errno while the coroutine is suspended. Store both in one signed word:
// non-negative values are byte counts, negative values are -errno, and
// INT64_MIN marks the not-yet-completed state.
class IoResultState {
public:
  static constexpr std::int64_t kPending = std::numeric_limits<std::int64_t>::min();

  [[nodiscard]]
  bool HasResult() const noexcept {
    return encoded_ != kPending;
  }

  void SetSuccess(std::size_t bytes) noexcept {
    ALYRN_CHECK(!HasResult(), "IoResultState result was set twice");
    ALYRN_CHECK(bytes <= static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()),
                   "IoResultState byte count cannot be encoded");
    encoded_ = static_cast<std::int64_t>(bytes);
  }

  void SetError(Error error) noexcept {
    ALYRN_CHECK(!HasResult(), "IoResultState result was set twice");
    ALYRN_CHECK(error.category() == std::system_category(),
                   "IoResultState only encodes system errors");
    const int value = error.value();
    ALYRN_CHECK(value > 0, "IoResultState cannot encode errno zero");
    encoded_ = -static_cast<std::int64_t>(value);
  }

  void SetResult(const Result<std::size_t>& result) noexcept {
    if (result.has_value()) {
      SetSuccess(*result);
    } else {
      SetError(result.error());
    }
  }

  [[nodiscard]]
  Result<std::size_t> Take() noexcept {
    ALYRN_CHECK(HasResult(), "IoResultState result was taken before completion");
    const std::int64_t encoded = std::exchange(encoded_, kPending);
    if (encoded >= 0) {
      return static_cast<std::size_t>(encoded);
    }
    return std::unexpected(Errno(static_cast<int>(-encoded)));
  }

private:
  std::int64_t encoded_{kPending};
};

static_assert(sizeof(IoResultState) == sizeof(std::int64_t));

}  // namespace alyrn::kqueue::detail
