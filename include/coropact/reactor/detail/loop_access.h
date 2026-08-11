// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/reactor/detail/loop_shutdown.h"
#include "coropact/reactor/loop.h"

namespace coropact::reactor::detail {

// Keeps EventLoop's resource-shutdown registry out of its public API while
// allowing loop-affine Reactor resources to join it.
class LoopAccess final {
public:
  static void RegisterShutdownParticipant(
      EventLoop& loop,
      LoopShutdownParticipant& participant) noexcept {
    loop.RegisterShutdownParticipant(participant);
  }

  static void UnregisterShutdownParticipant(
      EventLoop& loop,
      LoopShutdownParticipant& participant) noexcept {
    loop.UnregisterShutdownParticipant(participant);
  }
};

}  // namespace coropact::reactor::detail
