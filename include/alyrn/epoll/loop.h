// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <stop_token>
#include <vector>

#include "alyrn/backend/loop.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/epoll/detail/loop_shutdown.h"
#include "alyrn/detail/macros.h"
#include "alyrn/result.h"
#include "alyrn/time/clock.h"
#include "alyrn/time/timer_id.h"

namespace alyrn::epoll {

namespace detail {
class Channel;
class LoopAccess;
class Poller;
class TimerQueue;
}  // namespace detail

/*
 * Owner-thread epoll dispatcher. A Loop owns its poller, registered
 * channels, timers, and coroutine work. Cross-thread callers may request
 * stop, but may not submit or mutate owner-local I/O state directly.
 */
class Loop final : public coro::Scheduler {
public:
  using Functor = std::function<void()>;

  explicit Loop(std::pmr::memory_resource* frame_resource = nullptr) noexcept;
  ~Loop() noexcept override;

  ALYRN_DELETE_COPY_MOVE(Loop);

  // Runs the dispatcher until RequestStop() or token cancellation. Run() is
  // owner-thread-only. It returns only after the currently queued owner work
  // has drained. When stopping begins, registered Epoll resources are
  // synchronously asked to cancel their pending operations before that drain.
  void Run(std::stop_token token = {}) noexcept;

  // Requests dispatcher shutdown. This is thread-safe, idempotent, and wakes
  // an epoll_wait immediately. Cross-thread work goes through Post().
  // Registered owner-loop resources are asked to shut down by Run().
  void RequestStop() noexcept;

  // Thread-safe. Queues callback to run on the owner thread, inside this
  // Loop's scheduling context, on a later turn; callbacks posted from one
  // thread run in the order they were posted. A successful Post runs exactly
  // once: callbacks posted before the Loop finishes its shutdown drain still
  // run during that drain. Afterwards Post returns operation_canceled and the
  // callback is destroyed, unrun, on the calling thread. The Loop must outlive
  // every Post call; a Loop destroyed without running destroys queued
  // callbacks unrun. Callbacks run during shutdown must not start new I/O.
  [[nodiscard]]
  Result<void> Post(Functor callback);

  [[nodiscard]]
  backend::LoopState State() const noexcept {
    return state_.load(std::memory_order_acquire);
  }

  // Runs callback immediately on the owning loop thread, inside this Loop's
  // scheduling context: it may use owner-affine Channels and Spawn.
  void RunOnOwner(Functor callback) noexcept;

  // Schedules a coroutine work item for a later loop turn. The Loop is
  // itself the Scheduler; submission must happen on its owner thread.
  void Schedule(coro::Work* work) noexcept override;

  // Drains owner-local callbacks and coroutine work without polling. This is
  // used by worker shutdown after the stop token has ended the poll loop.
  void RunPending();

  // Returns true if the caller is running in the owning loop thread.
  [[nodiscard]]
  bool IsInLoopThread() const noexcept;

  // Timer callbacks run on the owner thread inside this Loop's scheduling
  // context, so they may use owner-affine Channels and Spawn.
  //
  // Schedules callback to run once at the specified time point.
  time::TimerId RunAt(time::Deadline deadline, Functor callback);

  // Schedules callback to run once after delay.
  time::TimerId RunAfter(time::Duration delay, Functor callback);

  // Schedules callback to run repeatedly every interval.
  time::TimerId RunEvery(time::Duration interval, Functor callback);

  // Cancels a previously scheduled timer.
  void Cancel(time::TimerId id);

private:
  friend class detail::Channel;
  friend class detail::LoopAccess;

  using Channel = detail::Channel;
  using LoopShutdownParticipant = detail::LoopShutdownParticipant;
  using LoopShutdownRegistry = detail::LoopShutdownRegistry;
  using Poller = detail::Poller;
  using TimerQueue = detail::TimerQueue;

  // Channel registration belongs to the implementation of Loop. These
  // methods are intentionally unavailable to Epoll callers.
  void UpdateChannel(Channel* channel);
  void RemoveChannel(Channel* channel);
  bool HasChannel(Channel* channel) const;
  void RegisterShutdownParticipant(LoopShutdownParticipant& participant) noexcept;
  void UnregisterShutdownParticipant(LoopShutdownParticipant& participant) noexcept;

  // Runs all work submitted through Schedule().
  void DoPendingWork();
  void BeginShutdown() noexcept;

  static void DispatchWakeup(void* context) noexcept;
  void DrainWakeup() noexcept;
  void Wakeup() noexcept;
  void RunPosted();
  void DrainPostedAndClose();
  void DetachWakeupChannel() noexcept;

  bool HasImmediateWork() const;

  bool looping_{false};
  std::atomic<backend::LoopState> state_{
      backend::LoopState::kCreated};

  std::unique_ptr<Poller> poller_;
  std::vector<Channel*> active_channels_;

  int wakeup_fd_{-1};
  std::unique_ptr<Channel> wakeup_channel_;

  coro::WorkQueue pending_work_;
  LoopShutdownRegistry shutdown_registry_;
  bool shutdown_started_{false};

  std::unique_ptr<TimerQueue> timer_queue_;

  std::mutex post_mutex_;
  std::vector<Functor> posted_;
  bool post_closed_{false};
};

static_assert(backend::ManagedLoop<Loop>);

}  // namespace alyrn::epoll
