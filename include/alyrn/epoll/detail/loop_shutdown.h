// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/detail/loop_shutdown.h"

namespace alyrn::epoll::detail {

// The registry is shared with the uring backend; epoll keeps its spelling.
using LoopShutdownParticipant = ::alyrn::detail::LoopShutdownParticipant;
using LoopShutdownRegistry = ::alyrn::detail::LoopShutdownRegistry;

}  // namespace alyrn::epoll::detail
