// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

// Public umbrella header for the epoll-based Reactor backend. It exports the
// event-loop and transport adapters, not the epoll implementation machinery.

#include "coropact/net/endpoint.h" // IWYU pragma: export
#include "coropact/net/net_utils.h" // IWYU pragma: export
#include "coropact/net/socket.h" // IWYU pragma: export
#include "coropact/reactor/connector.h" // IWYU pragma: export
#include "coropact/reactor/listener.h" // IWYU pragma: export
#include "coropact/reactor/loop.h" // IWYU pragma: export
#include "coropact/reactor/options.h" // IWYU pragma: export
#include "coropact/reactor/recv_source.h" // IWYU pragma: export
#include "coropact/reactor/stream.h" // IWYU pragma: export
