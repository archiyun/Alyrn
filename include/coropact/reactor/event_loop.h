// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "coropact/base/current_thread.h"
#include "coropact/coro/work.h"
#include "coropact/reactor/channel.h"
#include "coropact/time/timer_id.h"
#include "coropact/time/timestamp.h"
#include "coropact/utils/macros.h"

namespace coropact::coro {
class Scheduler;
}

namespace coropact::reactor {

class Poller;
class TimerQueue;
class EventLoopScheduler;

// EventLoop is the core event dispatcher in the Reactor model.
//
// Each EventLoop is bound to exactly one thread. It owns a Poller for waiting
// on I/O events, dispatches active Channel callbacks, runs queued functors in
// thread order, and manages timer callbacks through TimerQueue.
class EventLoop {
public:
  using Functor = std::function<void()>;

  EventLoop();
  ~EventLoop();

  COROPACT_DELETE_COPY_MOVE(EventLoop);

  // Starts the event loop and blocks until Quit() is requested.
  void Loop();

  // Requests the loop to exit. The loop stops after the current iteration.
  void Quit();

  [[nodiscard]]
  time::Timestamp PollReturnTime() const {
    return poll_return_time_;
  }

  // Runs cb immediately if called from the owning loop thread; otherwise,
  // schedules it to run in the loop thread. Thread-safe.
  void RunInLoop(Functor callback);

  // Queues cb to run in the loop thread on a later iteration. Thread-safe.
  void QueueInLoop(Functor callback);

  // Queues a non-owning coroutine Work item for the loop scheduler. Returns
  // false when work is null or is already queued. Thread-safe.
  bool QueueWork(coro::Work* work) noexcept;

  // The following Channel-management methods must be called from the owning
  // loop thread. They mutate the Poller's channel set and are not thread-safe.
  void UpdateChannel(Channel* channel);
  void RemoveChannel(Channel* channel);
  bool HasChannel(Channel* channel) const;

  // Returns true if the caller is running in the owning loop thread.
  [[nodiscard]]
  bool IsInLoopThread() const;

  // Schedules cb to run once at the specified time point.
  time::TimerId RunAt(time::Timestamp time, Functor callback);

  // Schedules cb to run once after delay_sec seconds.
  time::TimerId RunAfter(double delay_sec, Functor callback);

  // Schedules cb to run repeatedly every interval_sec seconds.
  time::TimerId RunEvery(double interval_sec, Functor callback);

  // Cancels a previously scheduled timer.
  void Cancel(time::TimerId id);

private:
  friend class EventLoopScheduler;

  // Wakes up the loop when work is queued from another thread.
  void Wakeup();

  // Handles readability on the wakeup fd.
  void HandleRead();
  static void DispatchWakeupRead(void* context, time::Timestamp receive_time) noexcept;

  // Runs all functors queued through QueueInLoop().
  void DoPendingFunctors();

  // Runs the Work items admitted through QueueWork().
  void DoPendingWork();

  void BindScheduler(coro::Scheduler* scheduler) noexcept;

  [[nodiscard]]
  bool HasImmediateWork();

  std::atomic<bool> looping_;
  std::atomic<bool> quit_;
  std::atomic<bool> calling_pending_functors_;

  const int thread_id_;
  time::Timestamp poll_return_time_;

  std::unique_ptr<Poller> poller_;
  std::vector<Channel*> active_channels_;

  // Eventfd or pipe-based wakeup mechanism used to interrupt epoll_wait when
  // another thread queues work into this loop.
  int wakeup_fd_;
  Channel wakeup_channel_;
  std::mutex wakeup_mutex_;

  std::mutex mutex_;
  std::vector<Functor> pending_functors_;
  coro::WorkQueue pending_work_;
  coro::Scheduler* work_scheduler_{nullptr};

  std::unique_ptr<TimerQueue> timer_queue_;
};

}  // namespace coropact::reactor
