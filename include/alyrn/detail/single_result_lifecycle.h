// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

#include "alyrn/detail/macros.h"

namespace alyrn::detail {

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

  void Reset() noexcept {
    state_ = State::kPending;
  }

  bool TryAuthorizeResult() noexcept {
    if (state_ != State::kPending) {
      return false;
    }
    state_ = State::kResultReady;
    return true;
  }

  bool TryAuthorizeRelease() noexcept {
    if (state_ != State::kResultReady) {
      return false;
    }
    state_ = State::kReleaseAuthorized;
    return true;
  }

  bool TryAuthorizeContinuation() noexcept {
    if (state_ != State::kReleaseAuthorized) {
      return false;
    }
    state_ = State::kContinuationAuthorized;
    return true;
  }

  bool ResultReady() const noexcept {
    return state_ != State::kPending;
  }

  bool ReleaseAuthorized() const noexcept {
    return state_ == State::kReleaseAuthorized || state_ == State::kContinuationAuthorized;
  }

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

}  // namespace alyrn::detail
