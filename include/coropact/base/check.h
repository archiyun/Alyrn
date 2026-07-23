// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <source_location>

#include "coropact/base/panic.h"

// Checks a non-recoverable program invariant in every build configuration.
// Recoverable failures from input, resource exhaustion, or system calls should
// be returned through coropact::base::Result instead.
#define COROPACT_CHECK(condition, message)                                               \
  do {                                                                               \
    if (!(condition)) [[unlikely]] {                                                 \
      ::coropact::base::Panic(#condition, (message), ::std::source_location::current()); \
    }                                                                                \
  } while (false)

// Checks an expensive diagnostic invariant in debug builds. Neither condition
// nor message is evaluated when NDEBUG is defined.
#ifndef NDEBUG
#define COROPACT_DCHECK(condition, message) COROPACT_CHECK(condition, message)
#else
#define COROPACT_DCHECK(condition, message) \
  do {                                  \
  } while (false)
#endif
