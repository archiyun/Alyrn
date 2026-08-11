// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

namespace coropact::reactor {

// Selects readiness delivery for a Reactor stream. The underlying poller is
// deliberately not part of this public option.
enum class TriggerMode : std::uint8_t {
  kLevelTriggered,
  kEdgeTriggered,
};

}  // namespace coropact::reactor
