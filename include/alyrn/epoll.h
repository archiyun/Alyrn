// SPDX-License-Identifier: MIT
#pragma once

#if !defined(__linux__)
#error "alyrn::epoll requires Linux epoll support"
#endif

// Public umbrella header for the Linux epoll backend. It exports the Loop,
// Runtime::Builder<runtime::Epoll>, and transport adapters, not the epoll
// implementation machinery.

#include "alyrn/epoll/connector.h"    // IWYU pragma: export
#include "alyrn/epoll/listener.h"     // IWYU pragma: export
#include "alyrn/epoll/loop.h"         // IWYU pragma: export
#include "alyrn/epoll/options.h"      // IWYU pragma: export
#include "alyrn/epoll/recv_source.h"  // IWYU pragma: export
#include "alyrn/epoll/runtime.h"      // IWYU pragma: export
#include "alyrn/epoll/stream.h"       // IWYU pragma: export
