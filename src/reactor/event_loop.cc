// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include <algorithm>

#include "coropact/base/check.h"
#include "coropact/base/current_thread.h"
#include "coropact/coro/scheduler.h"
#include "coropact/reactor/detail/channel.h"
#include "coropact/reactor/detail/poller.h"
#include "coropact/reactor/detail/timer_queue.h"
#include "coropact/reactor/loop.h"
#include "coropact/time/timer_id.h"

namespace coropact::reactor {

namespace {

static constexpr int kPollTimeMs = 10000;
thread_local EventLoop* t_loop_in_this_thread = nullptr;

}  // namespace

EventLoop::EventLoop(std::pmr::memory_resource* frame_resource)
    : Scheduler(frame_resource),
      thread_id_(base::tid()),
      poller_(detail::Poller::NewDefaultPoller(this)),
      timer_queue_(std::make_unique<detail::TimerQueue>(this)) {
  COROPACT_DCHECK(t_loop_in_this_thread == nullptr,
                  "EventLoop: only one EventLoop may exist per thread");
  t_loop_in_this_thread = this;
}

EventLoop::~EventLoop() {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop destructor called from wrong thread");
  COROPACT_DCHECK(!looping_, "EventLoop destroyed while looping");

  COROPACT_DCHECK(pending_work_.Empty(), "EventLoop destroyed with pending owner work");
  t_loop_in_this_thread = nullptr;
}

void EventLoop::Loop(std::stop_token token) {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop::Loop called from wrong thread");
  COROPACT_DCHECK(!looping_, "EventLoop::Loop called while already looping");

  looping_ = true;

  // Do not reset quit_ here: a quit request made by owner-thread setup before
  // Loop() begins must be honored.
  while (!quit_ && !token.stop_requested()) {
    DoPendingWork();

    // Quit() may have been called by pending work.
    // Do not enter a potentially blocking poll after the stop request.
    if (quit_ || token.stop_requested()) {
      break;
    }

    active_channels_.clear();

    const int timeout_ms =
        HasImmediateWork() ? 0 : (token.stop_possible() ? std::min(kPollTimeMs, 10) : kPollTimeMs);
    poller_->Poll(timeout_ms, &active_channels_);

    for (detail::Channel* channel : active_channels_) {
      channel->HandleEvent();
    }
  }

  looping_ = false;
}

void EventLoop::Quit() {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::Quit called from wrong thread");
  quit_ = true;
}

void EventLoop::RunOnOwner(Functor callback) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::RunOnOwner called from wrong thread");
  callback();
}

void EventLoop::Schedule(coro::Work* work) noexcept {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::Schedule called from wrong thread");
  COROPACT_CHECK(work != nullptr, "EventLoop::Schedule received null work");
  COROPACT_CHECK(pending_work_.PushBack(work),
                 "EventLoop::Schedule received work already in a queue");
}

void EventLoop::RunPending() {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::RunPending called from wrong thread");
  while (HasImmediateWork()) {
    DoPendingWork();
  }
}

void EventLoop::UpdateChannel(detail::Channel* channel) {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop::UpdateChannel called from wrong thread");
  poller_->UpdateChannel(channel);
}

void EventLoop::RemoveChannel(detail::Channel* channel) {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop::RemoveChannel called from wrong thread");
  poller_->RemoveChannel(channel);
}

bool EventLoop::HasChannel(detail::Channel* channel) const {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop::HasChannel called from wrong thread");
  return poller_->HasChannel(channel);
}

bool EventLoop::IsInLoopThread() const { return thread_id_ == base::tid(); }

void EventLoop::DoPendingWork() {
  if (pending_work_.Empty()) {
    return;
  }
  coro::WorkQueue work;
  work.Splice(pending_work_);
  RunBatch(work);
}

bool EventLoop::HasImmediateWork() const {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop::HasImmediateWork called from wrong thread");
  return !pending_work_.Empty();
}

time::TimerId EventLoop::RunAt(std::chrono::steady_clock::time_point time, Functor callback) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::RunAt called from wrong thread");
  return timer_queue_->AddTimer(std::move(callback), time,
                                std::chrono::steady_clock::duration::zero());
}

time::TimerId EventLoop::RunAfter(double delay, Functor callback) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::RunAfter called from wrong thread");
  const auto duration = std::chrono::duration_cast<std::chrono::steady_clock::duration>(
      std::chrono::duration<double>(delay));
  return timer_queue_->AddTimer(std::move(callback), std::chrono::steady_clock::now() + duration,
                                std::chrono::steady_clock::duration::zero());
}

time::TimerId EventLoop::RunEvery(double interval, Functor callback) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::RunEvery called from wrong thread");
  const auto duration = std::chrono::duration_cast<std::chrono::steady_clock::duration>(
      std::chrono::duration<double>(interval));
  return timer_queue_->AddTimer(std::move(callback), std::chrono::steady_clock::now() + duration,
                                duration);
}

void EventLoop::Cancel(time::TimerId id) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::Cancel called from wrong thread");
  timer_queue_->Cancel(id);
}

}  // namespace coropact::reactor
