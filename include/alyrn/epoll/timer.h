// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/result.h"
#include "alyrn/task.h"
#include "alyrn/time/clock.h"

namespace alyrn::epoll {

class Loop;

// Await on the owning Loop thread; positive waits require its active scheduler.
// Returns success after the delay (immediately for nonpositive delays), or
// operation_canceled if the Loop is stopping/stopped or stops before expiry.
// Once expiry fixes success, a later stop does not change that result.
// The Loop must outlive the task. Destruction before expiry on the owner thread
// removes the timer. Once a continuation is queued, its frame must remain alive
// until resumed; destroying it early is a checked contract violation.
[[nodiscard]] Task<Result<void>> SleepFor(Loop& loop, time::Duration delay);

}  // namespace alyrn::epoll
