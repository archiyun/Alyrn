// SPDX-License-Identifier: MIT
#include "alyrn/coro/scheduler.h"

namespace alyrn::coro {

// Defined out-of-line: Apple Clang rejects multiple inline thread_local
// definitions pulled from a static archive into one link unit.
thread_local Scheduler* Scheduler::current_ = nullptr;

}  // namespace alyrn::coro
