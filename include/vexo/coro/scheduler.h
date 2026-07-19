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

#include "vexo/coro/frame_allocator.h"
#include "vexo/coro/work.h"
#include "vexo/utils/macros.h"

namespace vexo::coro {

class Scheduler {
public:
  virtual ~Scheduler() = default;

  // Preconditions:
  // - work != nullptr
  // - work->run != nullptr
  // - work is not already enqueued
  // - work stays alive until it is run or cancelled by owner-side protocol
  virtual void Schedule(Work* work) noexcept = 0;

  // Runs a work item with this scheduler's frame resource active. Concrete
  // schedulers should use this wrapper instead of calling Work::Run() directly
  // so coroutine frames created during a resume inherit the selected resource.
  void Run(Work* work) noexcept {
    assert(work != nullptr);
    FrameAllocatorScope frame_scope{frame_resource_};
    work->Run();
  }

  std::pmr::memory_resource* frame_resource() const noexcept { return frame_resource_; }

  static Scheduler* Current() noexcept { return current_; }
  static Scheduler& RequireCurrent() noexcept {
    assert(current_ && "no current scheduler set for this thread");
    return *current_;
  }
  static void SetCurrent(Scheduler* scheduler) noexcept { current_ = scheduler; }

protected:
  explicit Scheduler(std::pmr::memory_resource* frame_resource = nullptr) noexcept
      : frame_resource_(frame_resource) {}
  VEXO_DELETE_COPY_MOVE(Scheduler);

private:
  static thread_local Scheduler* current_;
  std::pmr::memory_resource* frame_resource_{nullptr};
};

inline thread_local Scheduler* Scheduler::current_ = nullptr;

}  // namespace vexo::coro
