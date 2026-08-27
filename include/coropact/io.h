// SPDX-License-Identifier: MIT
#pragma once

// Public umbrella header for backend-neutral I/O contracts and algorithms.
// Concrete backends and profile/binding controls are included explicitly by
// composition roots.

#include "coropact/io/async_connector.h"  // IWYU pragma: export
#include "coropact/io/async_listener.h"   // IWYU pragma: export
#include "coropact/io/async_stream.h"     // IWYU pragma: export
#include "coropact/io/buffer.h"           // IWYU pragma: export
#include "coropact/io/read_into.h"        // IWYU pragma: export
#include "coropact/io/recv_source.h"      // IWYU pragma: export
