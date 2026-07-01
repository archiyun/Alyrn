// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "vexo/ds/intrusive_rbtree.h"
#include "vexo/time/timer.h"

namespace vexo::time {

inline bool TimerLess(const Timer* a, const Timer* b) {
  if (a->expiration() < b->expiration()) {
    return true;
  }
  if (a->expiration() > b->expiration()) {
    return false;
  }
  return a->sequence() < b->sequence();
}

using TimerTree = vexo::ds::IntrusiveRBTree<Timer, TimerLess>;

}  // namespace vexo::time
