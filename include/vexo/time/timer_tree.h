// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "vexo/ds/intrusive_rbtree.h"
#include "vexo/time/timer.h"

namespace vexo::time {

bool TimerLess(const Timer* a, const Timer* b);

using TimerTree = vexo::ds::IntrusiveRBTree<Timer, TimerLess>;

}  // namespace vexo::time
