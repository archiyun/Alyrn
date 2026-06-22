// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

// VEXO_ASSERT(cond, msg)
//
// Active only when NDEBUG is not defined (i.e., Debug builds).
// On failure: prints file/line/function context to stderr, then aborts.
// In Release builds the condition is evaluated but discarded; there is no
// runtime overhead beyond what the optimizer removes.
//
// Usage:
//   VEXO_ASSERT(reader_index_ <= writer_index_, "buffer invariant violated");

#ifndef NDEBUG
#include <cstdio>
#include <cstdlib>
#  define VEXO_ASSERT(cond, msg)                                       \
     do {                                                                  \
       if (__builtin_expect(!(cond), 0)) {                                 \
         ::fprintf(stderr,                                                 \
                   "\n[VEXO_ASSERT] %s\n"                              \
                   "  condition : %s\n"                                    \
                   "  location  : %s:%d (%s)\n",                          \
                   (msg), #cond, __FILE__, __LINE__, __func__);            \
         ::abort();                                                        \
       }                                                                   \
     } while (0)
#else
#  define VEXO_ASSERT(cond, msg) ((void)(cond))
#endif
