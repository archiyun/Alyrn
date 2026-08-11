// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#if !defined(__linux__)
#error "coropact::reactor requires Linux version support epoll"
#endif

// Public umbrella header for the epoll-based Reactor backend. It exports the
// EventLoop, Runtime::Builder<runtime::Reactor>, and transport adapters, not
// the epoll implementation machinery.

#include "coropact/reactor/connector.h"    // IWYU pragma: export
#include "coropact/reactor/listener.h"     // IWYU pragma: export
#include "coropact/reactor/loop.h"         // IWYU pragma: export
#include "coropact/reactor/options.h"      // IWYU pragma: export
#include "coropact/reactor/recv_source.h"  // IWYU pragma: export
#include "coropact/reactor/runtime.h"      // IWYU pragma: export
#include "coropact/reactor/stream.h"       // IWYU pragma: export
