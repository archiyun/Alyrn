// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/net/timer_rbtree.h"

#include "runtime/net/timer.h"

namespace runtime::net {

bool TimerLess(const Timer *a, const Timer *b) {
  if (a->expiration() < b->expiration()) return true;
  if (b->expiration() < a->expiration()) return false;
  return a->sequence() < b->sequence();
}

} // namespace runtime::net
