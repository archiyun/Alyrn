// SPDX-License-Identifier: MIT
#pragma once

#if !defined(__linux__)
#error "alyrn::luring requires Linux version support io_uring"
#endif

// Public umbrella header for the optional io_uring backend. It exports
// Loop, Runtime::Builder<runtime::LUring>, and transport adapters.

#include "alyrn/luring/connector.h"    // IWYU pragma: export
#include "alyrn/luring/listener.h"     // IWYU pragma: export
#include "alyrn/luring/loop.h"         // IWYU pragma: export
#include "alyrn/luring/options.h"      // IWYU pragma: export
#include "alyrn/luring/recv_source.h"  // IWYU pragma: export
#include "alyrn/luring/runtime.h"      // IWYU pragma: export
#include "alyrn/luring/stream.h"       // IWYU pragma: export
#include "alyrn/luring/timer.h"        // IWYU pragma: export
