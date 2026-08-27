// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

namespace alyrn::epoll {

// Selects readiness delivery for an epoll stream. The underlying poller is
// deliberately not part of this public option.
enum class TriggerMode : std::uint8_t {
  kLevelTriggered,
  kEdgeTriggered,
};

}  // namespace alyrn::epoll
