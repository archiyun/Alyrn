// SPDX-License-Identifier: MIT
#pragma once

#if !defined(__linux__)
#error "alyrn::uring requires Linux version support io_uring"
#endif

// Public umbrella header for the optional io_uring backend. It exports
// Loop, Runtime::Builder<runtime::Uring>, and transport adapters.

#include "alyrn/uring/connector.h"    // IWYU pragma: export
#include "alyrn/uring/listener.h"     // IWYU pragma: export
#include "alyrn/uring/loop.h"         // IWYU pragma: export
#include "alyrn/uring/options.h"      // IWYU pragma: export
#include "alyrn/uring/recv_source.h"  // IWYU pragma: export
#include "alyrn/uring/runtime.h"      // IWYU pragma: export
#include "alyrn/uring/stream.h"       // IWYU pragma: export
#include "alyrn/uring/timer.h"        // IWYU pragma: export
