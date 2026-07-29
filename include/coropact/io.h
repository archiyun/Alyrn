// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

// Public umbrella header for the I/O facade and its selected concrete
// backends. Backend implementations include the individual io/* contract
// headers instead, so including this facade does not create a dependency
// cycle.

#include "coropact/io/async_connector.h" // IWYU pragma: export
#include "coropact/io/async_listener.h" // IWYU pragma: export
#include "coropact/io/async_stream.h" // IWYU pragma: export
#include "coropact/io/accept_source.h" // IWYU pragma: export
#include "coropact/io/buffer.h" // IWYU pragma: export
#include "coropact/io/backend.h" // IWYU pragma: export
#include "coropact/io/profile.h" // IWYU pragma: export
#include "coropact/io/recv_source.h" // IWYU pragma: export
#include "coropact/io/stream_algorithms.h" // IWYU pragma: export

#include "coropact/reactor.h" // IWYU pragma: export

#if defined(COROPACT_ENABLE_URING) && COROPACT_ENABLE_URING
#include "coropact/luring.h" // IWYU pragma: export
#include "coropact/io/luring_backend.h" // IWYU pragma: export
#endif
