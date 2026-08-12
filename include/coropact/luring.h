// SPDX-License-Identifier: MIT
#pragma once

#if !defined(__linux__)
#error "coropact::luring requires Linux version support io_uring"
#endif

// Public umbrella header for the optional io_uring backend. It exports
// LUringLoop, Runtime::Builder<runtime::LUring>, and transport adapters.

#include "coropact/luring/connector.h"    // IWYU pragma: export
#include "coropact/luring/listener.h"     // IWYU pragma: export
#include "coropact/luring/loop.h"         // IWYU pragma: export
#include "coropact/luring/options.h"      // IWYU pragma: export
#include "coropact/luring/recv_source.h"  // IWYU pragma: export
#include "coropact/luring/runtime.h"      // IWYU pragma: export
#include "coropact/luring/stream.h"       // IWYU pragma: export
#include "coropact/luring/timer.h"        // IWYU pragma: export
