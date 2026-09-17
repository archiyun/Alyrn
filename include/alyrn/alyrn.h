// SPDX-License-Identifier: MIT
#pragma once

// Small application-facing Alyrn umbrella.  Domain-specific contracts and
// native adapters remain opt-in through io.h, net.h, time.h, and an adapter
// umbrella (epoll.h / uring.h). There is no alyrn/backend.h.
#include "alyrn/channel.h"    // IWYU pragma: export
#include "alyrn/result.h"
#include "alyrn/runtime.h"
#include "alyrn/scheduler.h"  // IWYU pragma: export
#include "alyrn/select.h"     // IWYU pragma: export
#include "alyrn/spawn.h"
#include "alyrn/task.h"
