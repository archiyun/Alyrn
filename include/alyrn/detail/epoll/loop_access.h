// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/detail/epoll/loop_shutdown.h"
#include "alyrn/epoll/loop.h"

namespace alyrn::epoll::detail {

// Keeps Loop's resource-shutdown registry out of its public API while
// allowing loop-affine Epoll resources to join it.
class LoopAccess final {
public:
  static void RegisterShutdownParticipant(
      Loop& loop,
      LoopShutdownParticipant& participant) noexcept {
    loop.RegisterShutdownParticipant(participant);
  }

  static void UnregisterShutdownParticipant(
      Loop& loop,
      LoopShutdownParticipant& participant) noexcept {
    loop.UnregisterShutdownParticipant(participant);
  }
};

}  // namespace alyrn::epoll::detail
