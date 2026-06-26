// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: Work, a vtable-free schedulable unit (a function
// pointer plus an intrusive next link the scheduler uses to chain entries), and
// ResumeWork, the adapter whose run() resumes a coroutine handle. Work itself
// does not know whether it drives a coroutine, and it owns no queue -- the queue
// belongs to whoever implements the Scheduler.
#pragma once

#include <coroutine>

namespace vexo::coro {

struct Work {
  using RunFn = void (*)(Work*) noexcept;

  RunFn run{nullptr};
  Work* next{nullptr};
};

// A Work that resumes a coroutine. This is the only place Work meets a frame.
struct ResumeWork : Work {
  std::coroutine_handle<> handle{};

  ResumeWork() noexcept { run = &ResumeWork::Resume; }
  explicit ResumeWork(std::coroutine_handle<> h) noexcept : handle(h) { run = &ResumeWork::Resume; }

private:
  static void Resume(Work* self) noexcept {
    // After resume() the frame (and possibly this ResumeWork) may be gone, so
    // touch nothing afterwards.
    static_cast<ResumeWork*>(self)->handle.resume();
  }
};

}  // namespace vexo::coro
