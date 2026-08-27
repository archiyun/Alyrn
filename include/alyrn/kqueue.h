// SPDX-License-Identifier: MIT
#pragma once

#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(__NetBSD__) && !defined(__OpenBSD__)
#error "alyrn::kqueue requires a BSD or Darwin host with kqueue"
#endif

// Public umbrella header for the kqueue readiness backend. It exports
// Loop, Runtime::Builder<runtime::Kqueue>, and transport adapters.

#include "alyrn/kqueue/connector.h"    // IWYU pragma: export
#include "alyrn/kqueue/listener.h"     // IWYU pragma: export
#include "alyrn/kqueue/loop.h"         // IWYU pragma: export
#include "alyrn/kqueue/options.h"      // IWYU pragma: export
#include "alyrn/kqueue/recv_source.h"  // IWYU pragma: export
#include "alyrn/kqueue/runtime.h"      // IWYU pragma: export
#include "alyrn/kqueue/stream.h"       // IWYU pragma: export
