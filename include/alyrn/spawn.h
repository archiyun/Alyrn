// SPDX-License-Identifier: MIT
#pragma once

// Application-facing task ownership and scheduling operations.
#include "alyrn/coro/spawn.h"
#include "alyrn/task.h"

namespace alyrn {

using DetachedTask = coro::DetachedTask;

template <class T>
  requires Returnable<T>
using JoinHandle = coro::JoinHandle<T>;

using coro::Spawn;
using coro::SpawnDetach;

}  // namespace alyrn
