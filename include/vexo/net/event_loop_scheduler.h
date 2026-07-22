// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "vexo/base/check.h"
#include "vexo/base/error.h"
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
  VEXO_DELETE_COPY(EventLoopScheduler);

  [[nodiscard]] static base::Result<EventLoopScheduler> Create(
      EventLoop* loop, std::pmr::memory_resource* frame_resource = nullptr) noexcept;

  explicit EventLoopScheduler(EventLoop* loop, std::pmr::memory_resource* frame_resource = nullptr);

  EventLoopScheduler(EventLoopScheduler&&) noexcept;
  EventLoopScheduler& operator=(EventLoopScheduler&&) noexcept;

  void Schedule(coro::Work* work) noexcept override {
    VEXO_DCHECK(work != nullptr, "EventLoopScheduler::Schedule: work must not be null");
    loop_->QueueInLoop([this, work] {
      coro::Scheduler* previous = coro::Scheduler::Current();
      coro::Scheduler::SetCurrent(this);
      auto restore = [previous] { coro::Scheduler::SetCurrent(previous); };

      Run(work);

      // Work may resume and destroy the coroutine frame it belongs to, so only
      // restore thread-local scheduler state after touching no work-owned data.
      restore();
    });
  }

  [[nodiscard]] EventLoop* loop() const noexcept { return loop_; }

private:
  EventLoop* loop_;
};

}  // namespace vexo::net
