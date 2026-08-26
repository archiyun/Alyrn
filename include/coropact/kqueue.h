// SPDX-License-Identifier: MIT
#pragma once

#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(__NetBSD__) && !defined(__OpenBSD__)
#error "coropact::kqueue requires a BSD or Darwin host with kqueue"
#endif

// Public umbrella header for the kqueue readiness backend. It exports
// Loop, Runtime::Builder<runtime::Kqueue>, and transport adapters.

#include "coropact/kqueue/connector.h"    // IWYU pragma: export
#include "coropact/kqueue/listener.h"     // IWYU pragma: export
#include "coropact/kqueue/loop.h"         // IWYU pragma: export
#include "coropact/kqueue/options.h"      // IWYU pragma: export
#include "coropact/kqueue/recv_source.h"  // IWYU pragma: export
#include "coropact/kqueue/runtime.h"      // IWYU pragma: export
#include "coropact/kqueue/stream.h"       // IWYU pragma: export
