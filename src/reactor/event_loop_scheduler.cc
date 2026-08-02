// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include "coropact/reactor/event_loop_scheduler.h"

#include <utility>

#include "coropact/base/error.h"

namespace coropact::reactor {

EventLoopScheduler::EventLoopScheduler(EventLoop* loop, std::pmr::memory_resource* frame_resource)
    : loop_(loop), Scheduler(frame_resource) {
  COROPACT_DCHECK(loop_ != nullptr, "EventLoopScheduler: loop must not be null");
  loop_->BindScheduler(this);
}

base::Result<EventLoopScheduler> EventLoopScheduler::Create(
    EventLoop* loop, std::pmr::memory_resource* frame_resource) noexcept {
  if (loop == nullptr) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  return EventLoopScheduler{loop, frame_resource};
}

EventLoopScheduler::EventLoopScheduler(EventLoopScheduler&& other) noexcept
    : Scheduler(other.FrameResource()), loop_(std::exchange(other.loop_, nullptr)) {
  if (loop_ != nullptr) {
    loop_->BindScheduler(this);
  }
}

EventLoopScheduler& EventLoopScheduler::operator=(EventLoopScheduler&& other) noexcept {
  if (this != &other) {
    loop_ = std::exchange(other.loop_, nullptr);
    SetFrameResource(other.FrameResource());
    if (loop_ != nullptr) {
      loop_->BindScheduler(this);
    }
  }
  return *this;
}

}  // namespace coropact::reactor
