// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

/**
 * @file coropact.h
 * @brief Public umbrella header for the CoroPact library.
 *
 * This header provides a convenient entry point for applications. Projects
 * that are sensitive to compile time can include individual module headers
 * instead. The optional io_uring backend is included only when enabled by the
 * build.
 */

#include "coropact/base.h"     // IWYU pragma: export
#include "coropact/coro.h"     // IWYU pragma: export
#include "coropact/ds.h"       // IWYU pragma: export
#include "coropact/io.h"       // IWYU pragma: export
#include "coropact/memory.h"   // IWYU pragma: export
#include "coropact/net.h"      // IWYU pragma: export
#include "coropact/reactor.h"  // IWYU pragma: export
#include "coropact/time.h"     // IWYU pragma: export
#include "coropact/utils.h"    // IWYU pragma: export

#if defined(COROPACT_ENABLE_URING) && COROPACT_ENABLE_URING
#include "coropact/luring.h"  // IWYU pragma: export
#endif

/**
 * @brief Main namespace for the CoroPact library.
 */
namespace coropact {}
