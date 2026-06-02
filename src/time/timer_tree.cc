// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/time/timer_tree.h"

#include "runtime/time/timer.h"

namespace runtime::time {

bool TimerLess(const Timer* a, const Timer* b) {
  if (a->expiration() < b->expiration()) return true;
  if (b->expiration() < a->expiration()) return false;
  return a->sequence() < b->sequence();
}

}  // namespace runtime::time
