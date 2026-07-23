// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/ds/intrusive_rbtree.h"
#include "coropact/time/timer.h"

namespace coropact::time {

inline bool TimerLess(const Timer* a, const Timer* b) {
  if (a->expiration() < b->expiration()) {
    return true;
  }
  if (a->expiration() > b->expiration()) {
    return false;
  }
  return a->sequence() < b->sequence();
}

using TimerTree = coropact::ds::IntrusiveRBTree<Timer, TimerLess>;

}  // namespace coropact::time
