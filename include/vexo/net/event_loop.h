// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "vexo/utils/macros.h"
#include "vexo/time/timer_id.h"
#include "vexo/time/timestamp.h"

namespace vexo::net {

class Channel;
class Poller;
class TimerQueue;

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

  VEXO_DELETE_COPY_MOVE(EventLoop);

  // Starts the event loop and blocks until Quit() is requested.
  void Loop();

  // Requests the loop to exit. The loop stops after the current iteration.
  void Quit();

  vexo::time::Timestamp poll_return_time() const { return poll_return_time_; }

  // Runs cb immediately if called from the owning loop thread; otherwise,
  // schedules it to run in the loop thread. Thread-safe.
  void RunInLoop(Functor cb);

  // Queues cb to run in the loop thread on a later iteration. Thread-safe.
  void QueueInLoop(Functor cb);

  // The following Channel-management methods must be called from the owning
  // loop thread. They mutate the Poller's channel set and are not thread-safe.
  void UpdateChannel(Channel* channel);
  void RemoveChannel(Channel* channel);
  bool HasChannel(Channel* channel) const;

  // Returns true if the caller is running in the owning loop thread.
  bool IsInLoopThread() const;

  // Schedules cb to run once at the specified time point.
  vexo::time::TimerId RunAt(vexo::time::Timestamp time, Functor cb);

  // Schedules cb to run once after delay_sec seconds.
  vexo::time::TimerId RunAfter(double delay_sec, Functor cb);

  // Schedules cb to run repeatedly every interval_sec seconds.
  vexo::time::TimerId RunEvery(double interval_sec, Functor cb);

  // Cancels a previously scheduled timer.
  void Cancel(vexo::time::TimerId id);

private:
  // Wakes up the loop when work is queued from another thread.
  void Wakeup();

  // Handles readability on the wakeup fd.
  void HandleRead();

  // Runs all functors queued through QueueInLoop().
  void DoPendingFunctors();

  std::atomic<bool> looping_;
  std::atomic<bool> quit_;
  std::atomic<bool> calling_pending_functors_;

  const std::thread::id thread_id_;
  vexo::time::Timestamp poll_return_time_;

  std::unique_ptr<Poller> poller_;
  std::vector<Channel*> active_channels_;

  // Eventfd or pipe-based wakeup mechanism used to interrupt epoll_wait when
  // another thread queues work into this loop.
  int wakeup_fd_;
  std::unique_ptr<Channel> wakeup_channel_;
  std::mutex wakeup_mutex_;

  std::mutex mutex_;
  std::vector<Functor> pending_functors_;

  std::unique_ptr<TimerQueue> timer_queue_;
};

}  // namespace vexo::net
