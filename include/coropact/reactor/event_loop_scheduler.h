// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/work.h"
#include "coropact/reactor/event_loop.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

// Adapts coropact::coro::Scheduler onto one EventLoop.
//
// This is the boundary between backend-neutral coroutine code and the Reactor
// loop. The coroutine layer submits Work*, and the EventLoop decides when that
// work runs on its owning thread.
class EventLoopScheduler final : public coro::Scheduler {
public:
  COROPACT_DELETE_COPY(EventLoopScheduler);

  [[nodiscard]]
  static base::Result<EventLoopScheduler> Create(
      EventLoop* loop, std::pmr::memory_resource* frame_resource = nullptr) noexcept;

  explicit EventLoopScheduler(EventLoop* loop, std::pmr::memory_resource* frame_resource = nullptr);

  EventLoopScheduler(EventLoopScheduler&&) noexcept;
  EventLoopScheduler& operator=(EventLoopScheduler&&) noexcept;

  void Schedule(coro::Work* work) noexcept override {
    COROPACT_DCHECK(work != nullptr, "EventLoopScheduler::Schedule: work must not be null");
    loop_->QueueInLoop([this, work] { Run(work); });
  }

  [[nodiscard]]
  EventLoop* Loop() const noexcept { return loop_; }

private:
  EventLoop* loop_;
};

}  // namespace coropact::reactor
