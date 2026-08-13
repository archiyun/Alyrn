// SPDX-License-Identifier: MIT
#include "coropact/coro/scheduler.h"

namespace coropact::coro {

// Defined out-of-line: Apple Clang rejects multiple inline thread_local
// definitions pulled from a static archive into one link unit.
thread_local Scheduler* Scheduler::current_ = nullptr;

}  // namespace coropact::coro
