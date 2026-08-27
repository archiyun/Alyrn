// SPDX-License-Identifier: MIT
#pragma once

// Legacy coroutine umbrella retained for source organization. Application
// code should prefer alyrn/alyrn.h or the root task/spawn headers; custom
// schedulers, Work, awaitable introspection, and frame-resource tuning remain
// available through their individual advanced headers.

#include "alyrn/spawn.h"              // IWYU pragma: export
#include "alyrn/task.h"               // IWYU pragma: export
#include "alyrn/coro/channel.h"       // IWYU pragma: export
#include "alyrn/coro/detached_task.h"  // IWYU pragma: export
#include "alyrn/coro/spawn.h"          // IWYU pragma: export
#include "alyrn/coro/sync_wait.h"      // IWYU pragma: export
#include "alyrn/coro/task.h"           // IWYU pragma: export
