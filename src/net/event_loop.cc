// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/event_loop.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include "vexo/log/logger.h"
#include "vexo/net/channel.h"
#include "vexo/net/poller.h"
#include "vexo/net/timer_queue.h"
#include "vexo/time/timer_id.h"
#include "vexo/time/timestamp.h"

namespace vexo::net {

namespace {

static constexpr int kPollTimeMs = 10000;

int CreateEventfd() {
  const int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (evtfd < 0) {
    LOG_FATALF("eventfd creation failed: errno={} message={}", errno,
               std::strerror(errno));
    std::abort();
  }
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

    LOG_ERRORF("eventfd write failed: fd={} errno={} message={}", fd, errno,
               std::strerror(errno));
    return;
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

    LOG_ERRORF("eventfd read failed: fd={} errno={} message={}", fd, errno,
               std::strerror(errno));
    return;
  }
}

thread_local EventLoop* t_loop_in_this_thread = nullptr;

}  // namespace

EventLoop::EventLoop()
    : looping_(false),
      quit_(false),
      calling_pending_functors_(false),
      thread_id_(std::this_thread::get_id()),
      poller_(Poller::NewDefaultPoller(this)),
      wakeup_fd_(CreateEventfd()),
      wakeup_channel_(std::make_unique<Channel>(this, wakeup_fd_)),
      timer_queue_(std::make_unique<TimerQueue>(this)) {
  assert(t_loop_in_this_thread == nullptr);
  t_loop_in_this_thread = this;

  // The wakeup fd is monitored like a normal Channel so other threads can
  // interrupt epoll_wait when they queue work into this loop.
  wakeup_channel_->set_read_callback([this](vexo::time::Timestamp) { HandleRead(); });
  wakeup_channel_->EnableReading();

  LOG_DEBUGF("event loop created: wakeup_fd={}", wakeup_fd_);
}

EventLoop::~EventLoop() {
  assert(IsInLoopThread());
  assert(!looping_);

  wakeup_channel_->DisableAll();
  wakeup_channel_->Remove();
  {
    std::lock_guard lk{wakeup_mutex_};
    if (wakeup_fd_ >= 0) {
      ::close(wakeup_fd_);
      wakeup_fd_ = -1;
    }
  }
  t_loop_in_this_thread = nullptr;
}

void EventLoop::Loop() {
  assert(IsInLoopThread());
  assert(!looping_);

  looping_.store(true, std::memory_order_relaxed);

  // Do not reset quit_ here: a Quit() that races in before Loop() begins (e.g.
  // another thread holds the loop pointer and stops it during startup) must be
  // honored, otherwise the loop would clear the request and block forever.
  LOG_INFOF("event loop entering loop");

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
  LOG_INFOF("event loop exited loop");
}

void EventLoop::Quit() {
  quit_.store(true, std::memory_order_relaxed);

  // If Quit() is called from another thread, wake up the loop so it can observe
  // the updated quit flag instead of staying blocked in epoll_wait.
  if (!IsInLoopThread()) {
    Wakeup();
  }
}

void EventLoop::RunInLoop(Functor cb) {
  if (IsInLoopThread()) {
    cb();
  } else {
    QueueInLoop(std::move(cb));
  }
}

void EventLoop::QueueInLoop(Functor cb) {
  {
    std::lock_guard lk{mutex_};
    pending_functors_.push_back(std::move(cb));
  }

  // Wake the loop when work is queued from another thread, or when the loop is
  // already executing pending functors and needs to observe newly queued work
  // in a later iteration.
  if (!IsInLoopThread() || calling_pending_functors_.load()) {
    Wakeup();
  }
}

bool EventLoop::HasImmediateWork() {
  assert(IsInLoopThread());

  {
    std::lock_guard lk{mutex_};
    return !pending_functors_.empty();
  }
}

void EventLoop::UpdateChannel(Channel* channel) {
  assert(IsInLoopThread());
  poller_->UpdateChannel(channel);
}

void EventLoop::RemoveChannel(Channel* channel) {
  assert(IsInLoopThread());
  poller_->RemoveChannel(channel);
}

bool EventLoop::HasChannel(Channel* channel) const {
  assert(IsInLoopThread());
  return poller_->HasChannel(channel);
}

bool EventLoop::IsInLoopThread() const { return thread_id_ == std::this_thread::get_id(); }

void EventLoop::Wakeup() {
  std::lock_guard lk{wakeup_mutex_};
  if (wakeup_fd_ >= 0) {
    WriteEventfd(wakeup_fd_);
  }
}

void EventLoop::HandleRead() { ReadEventfd(wakeup_fd_); }

void EventLoop::DoPendingFunctors() {
  std::vector<Functor> functors;
  calling_pending_functors_.store(true, std::memory_order_relaxed);

  {
    std::lock_guard lk{mutex_};
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

vexo::time::TimerId EventLoop::RunAt(vexo::time::Timestamp time, Functor cb) {
  return timer_queue_->AddTimer(std::move(cb), time, 0.0);
}

vexo::time::TimerId EventLoop::RunAfter(double delay, Functor cb) {
  using vexo::time::AddTime;
  using vexo::time::Timestamp;
  return timer_queue_->AddTimer(std::move(cb), AddTime(Timestamp::Now(), delay), 0.0);
}

vexo::time::TimerId EventLoop::RunEvery(double interval, Functor cb) {
  using vexo::time::AddTime;
  using vexo::time::Timestamp;
  return timer_queue_->AddTimer(std::move(cb), AddTime(Timestamp::Now(), interval), interval);
}

void EventLoop::Cancel(vexo::time::TimerId id) { timer_queue_->Cancel(id); }

}  // namespace vexo::net
