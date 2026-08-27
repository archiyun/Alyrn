// SPDX-License-Identifier: MIT
#pragma once

// Public umbrella header for backend-shared networking values. Raw POSIX
// socket operations are an advanced opt-in through net/native.h; event-source
// protocols are exposed through io.h or a concrete backend.

#include "alyrn/net/endpoint.h"     // IWYU pragma: export
#include "alyrn/net/tcp_options.h"  // IWYU pragma: export
