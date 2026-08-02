// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <memory_resource>
#include <stop_token>
#include <vector>

#include "coropact/base/current_thread.h"
#include "coropact/coro/scheduler.h"
#include "coropact/reactor/channel.h"
#include "coropact/time/timer_id.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

class Poller;
class TimerQueue;

// EventLoop is the core event dispatcher in the Reactor model.
//
// Each EventLoop is bound to exactly one thread. It owns a Poller for waiting
// on I/O events, dispatches active Channel callbacks, runs queued functors in
// thread order, and manages timer callbacks through TimerQueue.
class EventLoop final : public coro::Scheduler {
public:
  using Functor = std::function<void()>;

  explicit EventLoop(std::pmr::memory_resource* frame_resource = nullptr);
  ~EventLoop() override;

  COROPACT_DELETE_COPY_MOVE(EventLoop);

  // Starts the event loop and blocks until Quit() or stop_token is requested.
  // The stop token is intended for the owner of a worker thread; it avoids
  // making EventLoop a cross-thread callback transport.
  void Loop(std::stop_token token = {});

  // Requests the loop to exit. Must be called from the owning loop thread.
  void Quit();

  // Runs callback immediately on the owning loop thread.
  void RunOnOwner(Functor callback);

  // Schedules a coroutine work item for a later loop turn. The EventLoop is
  // itself the Scheduler; submission must happen on its owner thread.
  void Schedule(coro::Work* work) noexcept override;

  // Drains owner-local callbacks and coroutine work without polling. This is
  // used by worker shutdown after the stop token has ended the poll loop.
  void RunPending();

  // The following Channel-management methods must be called from the owning
  // loop thread. They mutate the Poller's channel set and are not thread-safe.
  void UpdateChannel(Channel* channel);
  void RemoveChannel(Channel* channel);
  bool HasChannel(Channel* channel) const;

  // Returns true if the caller is running in the owning loop thread.
  [[nodiscard]]
  bool IsInLoopThread() const;

  // Schedules callback to run once at the specified time point.
  time::TimerId RunAt(std::chrono::steady_clock::time_point time, Functor callback);

  // Schedules callback to run once after delay_sec seconds.
  time::TimerId RunAfter(double delay_sec, Functor callback);

  // Schedules callback to run repeatedly every interval_sec seconds.
  time::TimerId RunEvery(double interval_sec, Functor callback);

  // Cancels a previously scheduled timer.
  void Cancel(time::TimerId id);

private:
  // Runs all work submitted through Schedule().
  void DoPendingWork();

  [[nodiscard]]
  bool HasImmediateWork() const;

  bool looping_{false};
  bool quit_{false};

  const int thread_id_;
  std::unique_ptr<Poller> poller_;
  std::vector<Channel*> active_channels_;

  coro::WorkQueue pending_work_;

  std::unique_ptr<TimerQueue> timer_queue_;
};

}  // namespace coropact::reactor
