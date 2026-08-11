// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <system_error>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/error.h"

namespace coropact::reactor::detail {

// Reactor read/write attempts only need to retain a non-negative byte count or
// a positive system errno while the coroutine is suspended. Store both in one
// signed word: non-negative values are byte counts, negative values are
// -errno, and INT64_MIN marks the not-yet-completed state.
class ReactorIoResultState {
public:
  static constexpr std::int64_t kPending = std::numeric_limits<std::int64_t>::min();

  [[nodiscard]]
  bool HasResult() const noexcept {
    return encoded_ != kPending;
  }

  void SetSuccess(std::size_t bytes) noexcept {
    COROPACT_CHECK(!HasResult(), "ReactorIoResultState result was set twice");
    COROPACT_CHECK(bytes <= static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()),
                   "ReactorIoResultState byte count cannot be encoded");
    encoded_ = static_cast<std::int64_t>(bytes);
  }

  void SetError(base::Error error) noexcept {
    COROPACT_CHECK(!HasResult(), "ReactorIoResultState result was set twice");
    COROPACT_CHECK(error.category() == std::system_category(),
                   "ReactorIoResultState only encodes system errors");
    const int value = error.value();
    COROPACT_CHECK(value > 0, "ReactorIoResultState cannot encode errno zero");
    encoded_ = -static_cast<std::int64_t>(value);
  }

  void SetResult(const base::Result<std::size_t>& result) noexcept {
    if (result.has_value()) {
      SetSuccess(*result);
    } else {
      SetError(result.error());
    }
  }

  [[nodiscard]]
  base::Result<std::size_t> Take() noexcept {
    COROPACT_CHECK(HasResult(), "ReactorIoResultState result was taken before completion");
    const std::int64_t encoded = std::exchange(encoded_, kPending);
    if (encoded >= 0) {
      return static_cast<std::size_t>(encoded);
    }
    return std::unexpected(base::MakeErrno(static_cast<int>(-encoded)));
  }

private:
  std::int64_t encoded_{kPending};
};

static_assert(sizeof(ReactorIoResultState) == sizeof(std::int64_t));

}  // namespace coropact::reactor::detail
