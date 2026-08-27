// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/ds/intrusive_rbtree.h"
#include "coropact/time/timer.h"

namespace coropact::time {

// TimerTree is the rbtree adapter inside TimerIndex. Applications do not
// construct or insert into it; Loop::RunAfter owns that path.

inline bool TimerLess(const Timer* a, const Timer* b) {
  if (a->expiration() < b->expiration()) {
    return true;
  }
  if (a->expiration() > b->expiration()) {
    return false;
  }
  return a->sequence() < b->sequence();
}

using TimerTree = ds::IntrusiveTree<Timer, TimerLess>;

}  // namespace coropact::time
