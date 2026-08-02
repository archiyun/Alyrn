// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/reactor/event_loop.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>

#include "coropact/base/check.h"
#include "coropact/reactor/channel.h"
#include "coropact/reactor/poller.h"
#include "coropact/reactor/timer_queue.h"
#include "coropact/time/timer_id.h"
#include "coropact/time/timestamp.h"

namespace coropact::reactor {

namespace {

static constexpr int kPollTimeMs = 10000;

int CreateEventfd() {
  const int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  COROPACT_CHECK(evtfd >= 0, "EventLoop: eventfd creation failed");
  return evtfd;
}

void WriteEventfd(int fd) {
  const uint64_t one = 1;
  while (true) {
    const ssize_t n = ::write(fd, &one, sizeof(one));
    if (n == static_cast<ssize_t>(sizeof(one))) {
      return;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n < 0 && errno == EAGAIN) {
      return;
    }

    COROPACT_CHECK(false, "EventLoop: eventfd write failed");
  }
}

void ReadEventfd(int fd) {
  uint64_t one = 0;
  while (true) {
    const ssize_t n = ::read(fd, &one, sizeof(one));
    if (n == static_cast<ssize_t>(sizeof(one))) {
      return;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n < 0 && errno == EAGAIN) {
      return;
    }

    COROPACT_CHECK(false, "EventLoop: eventfd read failed");
  }
}

thread_local EventLoop* t_loop_in_this_thread = nullptr;

}  // namespace

EventLoop::EventLoop()
    : looping_(false),
      quit_(false),
      calling_pending_functors_(false),
      thread_id_(base::tid()),
      poller_(Poller::NewDefaultPoller(this)),
      wakeup_fd_(CreateEventfd()),
      wakeup_channel_(this, wakeup_fd_),
      timer_queue_(std::make_unique<TimerQueue>(this)) {
  COROPACT_DCHECK(t_loop_in_this_thread == nullptr,
                  "EventLoop: only one EventLoop may exist per thread");
  t_loop_in_this_thread = this;

  // The wakeup fd is monitored like a normal Channel so other threads can
  // interrupt epoll_wait when they queue work into this loop.
  wakeup_channel_.SetReadCallback(&EventLoop::DispatchWakeupRead, this);
  wakeup_channel_.EnableReading();
}

EventLoop::~EventLoop() {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop destructor called from wrong thread");
  COROPACT_DCHECK(!looping_, "EventLoop destroyed while looping");

  wakeup_channel_.DisableAll();
  wakeup_channel_.Remove();
  {
    std::lock_guard lock{wakeup_mutex_};
    if (wakeup_fd_ >= 0) {
      ::close(wakeup_fd_);
      wakeup_fd_ = -1;
    }
  }
  t_loop_in_this_thread = nullptr;
}

void EventLoop::Loop() {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop::Loop called from wrong thread");
  COROPACT_DCHECK(!looping_, "EventLoop::Loop called while already looping");

  looping_.store(true, std::memory_order_relaxed);

  // Do not reset quit_ here: a Quit() that races in before Loop() begins (e.g.
  // another thread holds the loop pointer and stops it during startup) must be
  // honored, otherwise the loop would clear the request and block forever.
  while (!quit_.load(std::memory_order_relaxed)) {
    DoPendingFunctors();

    // Quit() may have been called by a pending functor.
    // Do not enter a potentially blocking poll after the stop request.
    if (quit_.load(std::memory_order_relaxed)) {
      break;
    }

    active_channels_.clear();

    const int timeout_ms = HasImmediateWork() ? 0 : kPollTimeMs;
    poll_return_time_ = poller_->Poll(timeout_ms, &active_channels_);

    for (Channel* channel : active_channels_) {
      channel->HandleEvent(poll_return_time_);
    }
  }

  looping_.store(false, std::memory_order_relaxed);
}

void EventLoop::Quit() {
  quit_.store(true, std::memory_order_relaxed);

  // If Quit() is called from another thread, wake up the loop so it can observe
  // the updated quit flag instead of staying blocked in epoll_wait.
  if (!IsInLoopThread()) {
    Wakeup();
  }
}

void EventLoop::RunInLoop(Functor callback) {
  if (IsInLoopThread()) {
    callback();
  } else {
    QueueInLoop(std::move(callback));
  }
}

void EventLoop::QueueInLoop(Functor callback) {
  {
    std::lock_guard lock{mutex_};
    pending_functors_.push_back(std::move(callback));
  }
  // Wake the loop when work is queued from another thread, or when the loop is
  // already executing pending functors and needs to observe newly queued work
  // in a later iteration.
  if (!IsInLoopThread() || calling_pending_functors_.load()) {
    Wakeup();
  }
}

bool EventLoop::HasImmediateWork() {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop::HasImmediateWork called from wrong thread");

  std::lock_guard lock{mutex_};
  return !pending_functors_.empty();
}

void EventLoop::UpdateChannel(Channel* channel) {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop::UpdateChannel called from wrong thread");
  poller_->UpdateChannel(channel);
}

void EventLoop::RemoveChannel(Channel* channel) {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop::RemoveChannel called from wrong thread");
  poller_->RemoveChannel(channel);
}

bool EventLoop::HasChannel(Channel* channel) const {
  COROPACT_DCHECK(IsInLoopThread(), "EventLoop::HasChannel called from wrong thread");
  return poller_->HasChannel(channel);
}

bool EventLoop::IsInLoopThread() const { return thread_id_ == base::tid(); }

void EventLoop::Wakeup() {
  std::lock_guard lock{wakeup_mutex_};
  if (wakeup_fd_ >= 0) {
    WriteEventfd(wakeup_fd_);
  }
}

void EventLoop::HandleRead() { ReadEventfd(wakeup_fd_); }

void EventLoop::DispatchWakeupRead(void* context, time::Timestamp /*receive_time*/) noexcept {
  static_cast<EventLoop*>(context)->HandleRead();
}

void EventLoop::DoPendingFunctors() {
  std::vector<Functor> functors;
  calling_pending_functors_.store(true, std::memory_order_relaxed);

  {
    std::lock_guard lock{mutex_};
    functors.swap(pending_functors_);
  }

  // Move the pending queue into a local vector before running callbacks so
  // producers can continue to enqueue work without holding the mutex during
  // callback execution.
  for (auto& functor : functors) {
    functor();
  }

  calling_pending_functors_.store(false, std::memory_order_relaxed);
}

time::TimerId EventLoop::RunAt(time::Timestamp time, Functor callback) {
  return timer_queue_->AddTimer(std::move(callback), time, 0.0);
}

time::TimerId EventLoop::RunAfter(double delay, Functor callback) {
  return timer_queue_->AddTimer(std::move(callback), AddTime(time::Timestamp::Now(), delay), 0.0);
}

time::TimerId EventLoop::RunEvery(double interval, Functor callback) {
  return timer_queue_->AddTimer(std::move(callback), AddTime(time::Timestamp::Now(), interval),
                                interval);
}

void EventLoop::Cancel(time::TimerId id) { timer_queue_->Cancel(id); }

}  // namespace coropact::reactor
