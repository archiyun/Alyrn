// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: the Scheduler abstraction. Schedule(Work*) is the one
// coarse-grained virtual boundary this module allows (the no-vtable-on-the-hot-
// path rule covers per-resume/per-await code, not this submission edge).
// Current()/SetCurrent() publish the per-thread active scheduler so an awaiter
// can re-submit work without threading a pointer through every frame. The coro
// module never implements a concrete scheduler or owns a queue.
#pragma once

#include "vexo/coro/work.h"
#include "vexo/utils/macros.h"

namespace vexo::coro {

class Scheduler {
public:
  virtual ~Scheduler() = default;

  virtual void Schedule(Work* work) = 0;

  static Scheduler* Current() noexcept { return current_; }
  static void SetCurrent(Scheduler* scheduler) noexcept { current_ = scheduler; }

protected:
  Scheduler() = default;
  VEXO_DELETE_COPY_MOVE(Scheduler);

private:
  static thread_local Scheduler* current_;
};

inline thread_local Scheduler* Scheduler::current_ = nullptr;

}  // namespace vexo::coro
