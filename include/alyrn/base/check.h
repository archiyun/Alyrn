// SPDX-License-Identifier: MIT
#pragma once

#include <source_location>

#include "alyrn/base/panic.h"

// Checks a non-recoverable program invariant in every build configuration.
// Recoverable failures from input, resource exhaustion, or system calls should
// be returned through alyrn::Result instead.
#define ALYRN_CHECK(condition, message)                                               \
  do {                                                                               \
    if (!(condition)) [[unlikely]] {                                                 \
      ::alyrn::base::Panic(#condition, (message), ::std::source_location::current()); \
    }                                                                                \
  } while (false)

// Checks an expensive diagnostic invariant in debug builds. Neither condition
// nor message is evaluated when NDEBUG is defined.
#ifndef NDEBUG
#define ALYRN_DCHECK(condition, message) ALYRN_CHECK(condition, message)
#else
#define ALYRN_DCHECK(condition, message) \
  do {                                  \
  } while (false)
#endif
