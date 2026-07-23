// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: Work, a vtable-free schedulable unit (a function
// pointer plus an intrusive queue hook the scheduler uses to chain entries), and
// ResumeWork, the adapter whose run() resumes a coroutine handle. Work itself
// does not know whether it drives a coroutine, and it owns no queue -- the queue
// belongs to whoever implements the Scheduler.
#pragma once

#include <cassert>
#include <coroutine>

#include "coropact/ds/intrusive_queue.h"
#include "coropact/utils/macros.h"

namespace coropact::coro {

struct Work : public coropact::ds::QueueNode<Work> {
  COROPACT_DELETE_COPY_MOVE(Work);
  using RunFn = void (*)(Work*) noexcept;

  Work() = default;

  void Run() noexcept {
    assert(run && "Work::run is not be nullptr.");
    run(this);
  }

  RunFn run{nullptr};
};

using WorkQueue = coropact::ds::IntrusiveQueue<Work>;

// A Work that resumes a coroutine. This is the only place Work meets a frame.
struct ResumeWork : public Work {
  COROPACT_DELETE_COPY_MOVE(ResumeWork);
  std::coroutine_handle<> handle{};

  ResumeWork() noexcept { run = &ResumeWork::Resume; }
  explicit ResumeWork(std::coroutine_handle<> handle) noexcept : handle(handle) {
    run = &ResumeWork::Resume;
  }

private:
  static void Resume(Work* self) noexcept {
    auto* work = static_cast<ResumeWork*>(self);
    auto handle = work->handle;

    assert(handle && "ResumeWork requires a valid coroutine handle.");
    assert(!handle.done() && "cannot resume a completed coroutine.");
    handle.resume();
  }
};

}  // namespace coropact::coro
