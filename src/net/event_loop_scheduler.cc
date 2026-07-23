// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include "coropact/net/event_loop_scheduler.h"

#include "coropact/base/check.h"

namespace coropact::net {

EventLoopScheduler::EventLoopScheduler(EventLoop* loop, std::pmr::memory_resource* frame_resource)
    : loop_(loop), Scheduler(frame_resource) {}

base::Result<EventLoopScheduler> EventLoopScheduler::Create(
    EventLoop* loop, std::pmr::memory_resource* frame_resource) noexcept {
  if (loop == nullptr) {
    return std::unexpected(base::make_errno(EINVAL));
  }
  return EventLoopScheduler{loop, frame_resource};
}

EventLoopScheduler::EventLoopScheduler(EventLoopScheduler&& other) noexcept
    : loop_(std::exchange(other.loop_, nullptr)) {}

EventLoopScheduler& EventLoopScheduler::operator=(EventLoopScheduler&& other) noexcept {
  if (this != &other) {
    loop_ = std::exchange(other.loop_, nullptr);
  }
  return *this;
}

}  // namespace coropact::net