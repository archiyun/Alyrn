// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/net/timer_rbtree.h"

#include "runtime/net/timer.h"

namespace runtime::net {

bool TimerLess(const Timer *a, const Timer *b) {
  if (a->Expiration() < b->Expiration()) return true;
  if (b->Expiration() < a->Expiration()) return false;
  return a->Sequence() < b->Sequence();
}

} // namespace runtime::net
