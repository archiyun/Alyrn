// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <memory_resource>
#include <stop_token>
#include <vector>

#include "coropact/backend/loop.h"
#include "coropact/base/current_thread.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/work.h"
#include "coropact/kqueue/detail/loop_shutdown.h"
#include "coropact/time/clock.h"
#include "coropact/time/timer_id.h"
#include "coropact/utils/macros.h"

namespace coropact::kqueue {

namespace detail {
class Channel;
class KqueuePoller;
class LoopAccess;
class TimerQueue;
}  // namespace detail

/*
 * Owner-thread kqueue dispatcher. A KqueueLoop owns its poller, registered
 * channels, and coroutine work. Cross-thread callers may request stop, but may
 * not submit or mutate owner-local I/O state directly.
 *
 * This is a parallel backend to the Linux epoll Reactor, not a portable
 * variant of it. It refines the same logical lifecycle contracts without
 * sharing an implementation.
 */
class KqueueLoop final : public coro::Scheduler {
public:
  using Functor = std::function<void()>;

  explicit KqueueLoop(std::pmr::memory_resource* frame_resource = nullptr);
  ~KqueueLoop() override;

  COROPACT_DELETE_COPY_MOVE(KqueueLoop);

  // Runs the dispatcher until RequestStop() or token cancellation. Run() is
  // owner-thread-only. It returns only after the currently queued owner work
  // has drained. When stopping begins, registered kqueue resources are
  // synchronously asked to cancel their pending operations before that drain.
  void Run(std::stop_token token = {});

  // Requests dispatcher shutdown. This is thread-safe, idempotent, and wakes a
  // blocked kevent immediately. It is intentionally not a cross-thread work
  // queue. Registered owner-loop resources are asked to shut down by Run().
  void RequestStop() noexcept;

  [[nodiscard]]
  backend::LoopState State() const noexcept {
    return state_.load(std::memory_order_acquire);
  }

  // Runs callback immediately on the owning loop thread.
  void RunOnOwner(Functor callback);

  // Schedules a coroutine work item for a later loop turn. The KqueueLoop is
  // itself the Scheduler; submission must happen on its owner thread.
  void Schedule(coro::Work* work) noexcept override;

  // Drains owner-local coroutine work without polling. This is used by worker
  // shutdown after the stop token has ended the poll loop.
  void RunPending();

  // Returns true if the caller is running in the owning loop thread.
  [[nodiscard]]
  bool IsInLoopThread() const noexcept;

  // Schedules callback to run once at the specified time point. Owner-thread only.
  time::TimerId RunAt(time::Deadline deadline, Functor callback);

  // Schedules callback to run once after delay. Owner-thread only.
  time::TimerId RunAfter(time::Duration delay, Functor callback);

  // Schedules callback to run repeatedly every interval. Owner-thread only.
  time::TimerId RunEvery(time::Duration interval, Functor callback);

  // Cancels a previously scheduled timer. Owner-thread only.
  void Cancel(time::TimerId id);

private:
  friend class detail::Channel;
  friend class detail::LoopAccess;

  using Channel = detail::Channel;
  using KqueuePoller = detail::KqueuePoller;
  using LoopShutdownParticipant = detail::LoopShutdownParticipant;
  using TimerQueue = detail::TimerQueue;

  // Channel registration belongs to the implementation of KqueueLoop. A
  // registration is keyed by (fd, filter), so one call here may translate into
  // several kevent changes. These methods are intentionally unavailable to
  // kqueue callers.
  void UpdateChannel(Channel* channel);
  void RemoveChannel(Channel* channel);
  [[nodiscard]]
  bool HasChannel(Channel* channel) const;

  void RegisterShutdownParticipant(LoopShutdownParticipant& participant) noexcept;
  void UnregisterShutdownParticipant(LoopShutdownParticipant& participant) noexcept;

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

  const base::ThreadId thread_id_;
  std::unique_ptr<detail::KqueuePoller> poller_;
  std::unique_ptr<detail::TimerQueue> timer_queue_;
  std::vector<detail::Channel*> active_channels_;

  /* A self-pipe rather than an eventfd or EVFILT_USER: it is portable across
   * every kqueue host, and macOS has no pipe2() to make the flags atomic. */
  int wakeup_read_fd_{-1};
  int wakeup_write_fd_{-1};
  std::unique_ptr<detail::Channel> wakeup_channel_;

  coro::WorkQueue pending_work_;
  detail::LoopShutdownRegistry shutdown_registry_;
  bool shutdown_started_{false};
};

static_assert(backend::ManagedLoop<KqueueLoop>);

}  // namespace coropact::kqueue
