// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/base/rbtree.h"
#include "runtime/net/timer.h"
namespace runtime::net {

  bool TimerLess(const Timer* a, const Timer* b);
  using TimerTree = runtime::base::IntrusiveRBTree<Timer, &Timer::tree_node_, TimerLess>;
  
} // namespace runtime::net

