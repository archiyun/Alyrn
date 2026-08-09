// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <memory_resource>
#include <stop_token>
#include <vector>

#include "coropact/backend/loop.h"
#include "coropact/coro/scheduler.h"
#include "coropact/reactor/detail/loop_shutdown.h"
#include "coropact/time/timer_id.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

namespace detail {
class Channel;
class LoopAccess;
class Poller;
class TimerQueue;
}  // namespace detail

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

  // Runs the dispatcher until RequestStop() or token cancellation. Run() is
  // owner-thread-only. It returns only after the currently queued owner work
  // has drained. When stopping begins, registered Reactor resources are
  // synchronously asked to cancel their pending operations before that drain.
  void Run(std::stop_token token = {});

  // Requests dispatcher shutdown. This is thread-safe, idempotent, and wakes
  // an epoll_wait immediately. It is intentionally not a cross-thread work
  // queue. Registered owner-loop resources are asked to shut down by Run().
  void RequestStop() noexcept;

  [[nodiscard]]
  backend::LoopState State() const noexcept {
    return state_.load(std::memory_order_acquire);
  }

  // Runs callback immediately on the owning loop thread.
  void RunOnOwner(Functor callback);

  // Schedules a coroutine work item for a later loop turn. The EventLoop is
  // itself the Scheduler; submission must happen on its owner thread.
  void Schedule(coro::Work* work) noexcept override;

  // Drains owner-local callbacks and coroutine work without polling. This is
  // used by worker shutdown after the stop token has ended the poll loop.
  void RunPending();

  // Returns true if the caller is running in the owning loop thread.
  [[nodiscard]]
  bool IsInLoopThread() const noexcept;

  // Schedules callback to run once at the specified time point.
  time::TimerId RunAt(std::chrono::steady_clock::time_point time, Functor callback);

  // Schedules callback to run once after delay_sec seconds.
  time::TimerId RunAfter(double delay_sec, Functor callback);

  // Schedules callback to run repeatedly every interval_sec seconds.
  time::TimerId RunEvery(double interval_sec, Functor callback);

  // Cancels a previously scheduled timer.
  void Cancel(time::TimerId id);

private:
  friend class detail::Channel;
  friend class detail::LoopAccess;

  // Channel registration belongs to the implementation of EventLoop. These
  // methods are intentionally unavailable to Reactor callers.
  void UpdateChannel(detail::Channel* channel);
  void RemoveChannel(detail::Channel* channel);
  bool HasChannel(detail::Channel* channel) const;
  void RegisterShutdownParticipant(detail::LoopShutdownParticipant& participant) noexcept;
  void UnregisterShutdownParticipant(detail::LoopShutdownParticipant& participant) noexcept;

  // Runs all work submitted through Schedule().
  void DoPendingWork();
  void BeginShutdown() noexcept;

  static void DispatchWakeup(void* context) noexcept;
  void DrainWakeup() noexcept;
  void Wakeup() noexcept;
  void DetachWakeupChannel() noexcept;

  [[nodiscard]]
  bool HasImmediateWork() const;

  bool looping_{false};
  std::atomic<backend::LoopState> state_{backend::LoopState::kCreated};

  const int thread_id_;
  std::unique_ptr<detail::Poller> poller_;
  std::vector<detail::Channel*> active_channels_;

  int wakeup_fd_{-1};
  std::unique_ptr<detail::Channel> wakeup_channel_;

  coro::WorkQueue pending_work_;
  detail::LoopShutdownRegistry shutdown_registry_;
  bool shutdown_started_{false};

  std::unique_ptr<detail::TimerQueue> timer_queue_;
};

}  // namespace coropact::reactor
