// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

// CTRACK is intentionally optional. Keep the hot-path source readable while
// making the profiling build completely compile-time removable.
#if defined(VEXO_ENABLE_CTRACK)
#include <ctrack.hpp>
#define VEXO_CTRACK_SCOPE(name) CTRACK_NAME(name)
#else
#define VEXO_CTRACK_SCOPE(name) \
  do {                           \
  } while (false)
#endif
