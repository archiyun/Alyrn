// SPDX-License-Identifier: MIT
#pragma once

// Small application-facing Alyrn umbrella.  Domain-specific contracts and
// native backend adapters remain opt-in through io.h, net.h, time.h, and a
// backend umbrella.
#include "alyrn/result.h"
#include "alyrn/runtime.h"
#include "alyrn/spawn.h"
#include "alyrn/task.h"
