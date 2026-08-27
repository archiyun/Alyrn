// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/ds/intrusive_rbtree.h"
#include "alyrn/time/timer.h"

namespace alyrn::time {

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

}  // namespace alyrn::time
