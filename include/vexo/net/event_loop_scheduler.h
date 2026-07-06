// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>

#include "vexo/coro/scheduler.h"
#include "vexo/coro/work.h"
#include "vexo/net/event_loop.h"
#include "vexo/utils/macros.h"

namespace vexo::net {

// Adapts vexo::coro::Scheduler onto one EventLoop.
//
// This is the boundary between backend-neutral coroutine code and the Reactor
// loop. The coroutine layer submits Work*, and the EventLoop decides when that
// work runs on its owning thread.
class EventLoopScheduler final : public coro::Scheduler {
public:
  explicit EventLoopScheduler(EventLoop* loop) noexcept : loop_(loop) { assert(loop_ != nullptr); }
  VEXO_DELETE_COPY_MOVE(EventLoopScheduler);

  void Schedule(coro::Work* work) noexcept override {
    assert(work != nullptr);
    loop_->QueueInLoop([this, work] {
      coro::Scheduler* previous = coro::Scheduler::Current();
      coro::Scheduler::SetCurrent(this);
      auto restore = [previous] { coro::Scheduler::SetCurrent(previous); };

      work->Run();

      // Work may resume and destroy the coroutine frame it belongs to, so only
      // restore thread-local scheduler state after touching no work-owned data.
      restore();
    });
  }

  EventLoop* loop() const noexcept { return loop_; }

private:
  EventLoop* loop_;
};
}  // namespace vexo::net
