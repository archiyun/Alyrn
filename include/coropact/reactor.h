// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

// Public umbrella header for the epoll-based Reactor backend.

#include "coropact/net/endpoint.h" // IWYU pragma: export
#include "coropact/net/net_utils.h" // IWYU pragma: export
#include "coropact/net/socket.h" // IWYU pragma: export
#include "coropact/reactor/channel.h" // IWYU pragma: export
#include "coropact/reactor/epoll_poller.h" // IWYU pragma: export
#include "coropact/reactor/event_loop.h" // IWYU pragma: export
#include "coropact/reactor/event_loop_scheduler.h" // IWYU pragma: export
#include "coropact/reactor/poller.h" // IWYU pragma: export
#include "coropact/reactor/reactor_connect.h" // IWYU pragma: export
#include "coropact/reactor/reactor_listener.h" // IWYU pragma: export
#include "coropact/reactor/reactor_stream.h" // IWYU pragma: export
#include "coropact/reactor/timer_queue.h" // IWYU pragma: export
#include "coropact/reactor/reactor_worker.h" // IWYU pragma: export
#include "coropact/reactor/reactor_worker_group.h" // IWYU pragma: export
