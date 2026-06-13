// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/ds/intrusive_rbtree.h"
#include "runtime/time/timer.h"

namespace runtime::time {

bool TimerLess(const Timer* a, const Timer* b);

using TimerTree = runtime::ds::IntrusiveRBTree<Timer, TimerLess>;

}  // namespace runtime::time
