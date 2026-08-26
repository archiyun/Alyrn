// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/kqueue/detail/loop_shutdown.h"
#include "coropact/kqueue/loop.h"

namespace coropact::kqueue::detail {

// Keeps Loop's resource-shutdown registry out of its public API while
// allowing loop-affine kqueue resources to join it.
class LoopAccess final {
public:
  static void RegisterShutdownParticipant(Loop& loop,
                                          LoopShutdownParticipant& participant) noexcept {
    loop.RegisterShutdownParticipant(participant);
  }

  static void UnregisterShutdownParticipant(Loop& loop,
                                            LoopShutdownParticipant& participant) noexcept {
    loop.UnregisterShutdownParticipant(participant);
  }
};

}  // namespace coropact::kqueue::detail
