// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/time/timer_tree.h"

#include "vexo/time/timer.h"

namespace vexo::time {

bool TimerLess(const Timer* a, const Timer* b) {
  if (a->expiration() < b->expiration()) return true;
  if (b->expiration() < a->expiration()) return false;
  return a->sequence() < b->sequence();
}

}  // namespace vexo::time
