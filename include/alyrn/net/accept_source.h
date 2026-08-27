// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>

namespace alyrn::net {

// Admission and buffering limits shared by Epoll and uring AcceptSource
// implementations. pending_depth limits physical accept requests; the event
// capacity limits accepted Stream values that can wait for the consumer.
struct AcceptSourceOptions {
  std::size_t pending_depth{4};
  std::size_t event_capacity{64};
  // Zero selects event_capacity / 2. The threshold must stay below the
  // admission capacity so a paused source has room to re-arm safely.
  std::size_t resume_threshold{0};

  [[nodiscard]]
  constexpr bool Valid() const noexcept {
    return pending_depth > 0 && event_capacity >= pending_depth &&
           (resume_threshold == 0 || resume_threshold < event_capacity);
  }

  [[nodiscard]]
  constexpr std::size_t ResumeThreshold() const noexcept {
    return resume_threshold == 0 ? event_capacity / 2 : resume_threshold;
  }
};

}  // namespace alyrn::net

// Backends include this implementation header because they store the state
// machine by value. Application code only needs AcceptSourceOptions.
#include "alyrn/detail/net/accept_source_state.h"
