// SPDX-License-Identifier: MIT
#pragma once

// Public umbrella header for backend-neutral I/O contracts and algorithms.
// Concrete backends and profile/binding controls are included explicitly by
// composition roots.

#include "alyrn/io/accept_source.h"   // IWYU pragma: export
#include "alyrn/io/async_connector.h" // IWYU pragma: export
#include "alyrn/io/async_listener.h"  // IWYU pragma: export
#include "alyrn/io/async_stream.h"    // IWYU pragma: export
#include "alyrn/io/buffer.h"          // IWYU pragma: export
#include "alyrn/io/loop.h"            // IWYU pragma: export
#include "alyrn/io/read_into.h"       // IWYU pragma: export
#include "alyrn/io/recv_source.h"     // IWYU pragma: export
