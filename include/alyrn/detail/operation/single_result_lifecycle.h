// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

#include "alyrn/detail/utils/macros.h"

namespace alyrn::detail::operation {

/*
 * Lifecycle protocol for one single-result operation with coupled resource
 * release. The adapter owns the result and physical resources; this type only
 * enforces their logical ordering:
 *
 *   result ready -> release authorized -> continuation authorized
 *
 * The object is thread-confined and intentionally has no I/O dependency. A
 * backend records the result, releases its slot or buffer, then schedules the
 * coroutine in that order.
 */
class SingleResultLifecycle {
public:
  ALYRN_DELETE_COPY_MOVE(SingleResultLifecycle);

  SingleResultLifecycle() noexcept = default;

  [[nodiscard]]
  bool TryAuthorizeResult() noexcept {
    if (state_ != State::kPending) {
      return false;
    }
    state_ = State::kResultReady;
    return true;
  }

  [[nodiscard]]
  bool TryAuthorizeRelease() noexcept {
    if (state_ != State::kResultReady) {
      return false;
    }
    state_ = State::kReleaseAuthorized;
    return true;
  }

  [[nodiscard]]
  bool TryAuthorizeContinuation() noexcept {
    if (state_ != State::kReleaseAuthorized) {
      return false;
    }
    state_ = State::kContinuationAuthorized;
    return true;
  }

  [[nodiscard]]
  bool ResultReady() const noexcept {
    return state_ != State::kPending;
  }

  [[nodiscard]]
  bool ReleaseAuthorized() const noexcept {
    return state_ == State::kReleaseAuthorized || state_ == State::kContinuationAuthorized;
  }

  [[nodiscard]]
  bool ContinuationAuthorized() const noexcept {
    return state_ == State::kContinuationAuthorized;
  }

private:
  enum class State : std::uint8_t {
    kPending,
    kResultReady,
    kReleaseAuthorized,
    kContinuationAuthorized,
  };

  State state_{State::kPending};
};

static_assert(sizeof(SingleResultLifecycle) == 1);

}  // namespace alyrn::detail::operation
