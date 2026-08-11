// SPDX-License-Identifier: MIT
#pragma once

// Public umbrella header for application coroutine code. Custom schedulers,
// Work, awaitable introspection, and frame-resource tuning remain available
// through their individual headers for runtime implementers.

#include "coropact/coro/detached_task.h"  // IWYU pragma: export
#include "coropact/coro/spawn.h"          // IWYU pragma: export
#include "coropact/coro/sync_wait.h"      // IWYU pragma: export
#include "coropact/coro/task.h"           // IWYU pragma: export
