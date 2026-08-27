// SPDX-License-Identifier: MIT
#pragma once

#if !defined(__linux__)
#error "alyrn::reactor requires Linux version support epoll"
#endif

// Public umbrella header for the epoll-based Reactor backend. It exports the
// Loop, Runtime::Builder<runtime::Reactor>, and transport adapters, not
// the epoll implementation machinery.

#include "alyrn/reactor/connector.h"    // IWYU pragma: export
#include "alyrn/reactor/listener.h"     // IWYU pragma: export
#include "alyrn/reactor/loop.h"         // IWYU pragma: export
#include "alyrn/reactor/options.h"      // IWYU pragma: export
#include "alyrn/reactor/recv_source.h"  // IWYU pragma: export
#include "alyrn/reactor/runtime.h"      // IWYU pragma: export
#include "alyrn/reactor/stream.h"       // IWYU pragma: export
