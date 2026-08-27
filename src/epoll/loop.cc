// SPDX-License-Identifier: MIT
#include "alyrn/epoll/loop.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>

#include "alyrn/detail/base/check.h"
#include "alyrn/detail/base/current_thread.h"
#include "alyrn/coro/frame_allocator.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/detail/epoll/channel.h"
#include "alyrn/detail/epoll/poller.h"
#include "alyrn/detail/epoll/timer_queue.h"
#include "alyrn/time/timer_id.h"

namespace alyrn::epoll {

namespace {

constexpr int kPollTimeMs = 10000;
thread_local Loop* t_loop_in_this_thread = nullptr;

}  // namespace

Loop::Loop(std::pmr::memory_resource* frame_resource) noexcept
    : Scheduler(frame_resource),
      thread_id_(::alyrn::detail::CurrentThreadId()),
      poller_(Poller::NewDefaultPoller()),
      timer_queue_(std::make_unique<TimerQueue>(this)) {
  ALYRN_CHECK(t_loop_in_this_thread == nullptr, "Loop: only one Loop may exist per thread");
  t_loop_in_this_thread = this;

  wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  ALYRN_CHECK(wakeup_fd_ >= 0, "Loop: eventfd creation failed");
  wakeup_channel_ = std::make_unique<Channel>(this, wakeup_fd_);
  wakeup_channel_->SetReadCallback(&Loop::DispatchWakeup, this);
  wakeup_channel_->EnableReading();
}

Loop::~Loop() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop destructor called from wrong thread");
  ALYRN_CHECK(!looping_, "Loop destroyed while looping");

  ALYRN_CHECK(pending_work_.Empty(), "Loop destroyed with pending owner work");
  ALYRN_CHECK(shutdown_registry_.Empty(), "Loop destroyed with registered shutdown resources");
  DetachWakeupChannel();
  if (wakeup_fd_ >= 0) {
    ::close(wakeup_fd_);
  }
  t_loop_in_this_thread = nullptr;
}

void Loop::Run(std::stop_token token) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::Run called from wrong thread");
  ALYRN_CHECK(!looping_, "Loop::Run called while already running");

  ::alyrn::detail::backend::LoopState expected = ::alyrn::detail::backend::LoopState::kCreated;
  if (!state_.compare_exchange_strong(expected, ::alyrn::detail::backend::LoopState::kRunning,
                                      std::memory_order_acq_rel, std::memory_order_acquire)) {
    ALYRN_CHECK(expected == ::alyrn::detail::backend::LoopState::kStopping,
                   "Loop::Run may only run a created or stopping loop");
  }

  looping_ = true;
  std::stop_callback on_stop{token, [this] { RequestStop(); }};

  while (State() == ::alyrn::detail::backend::LoopState::kRunning) {
    DoPendingWork();

    if (State() != ::alyrn::detail::backend::LoopState::kRunning) {
      break;
    }

    active_channels_.clear();

    const int timeout_ms = HasImmediateWork() ? 0 : kPollTimeMs;
    poller_->Poll(timeout_ms, &active_channels_);

    for (Channel* channel : active_channels_) {
      channel->HandleEventUnchecked();
    }
    coro::CoroFramePoolResource::DrainCurrent();
  }

  BeginShutdown();
  RunPending();
  coro::CoroFramePoolResource::DrainCurrent();
  looping_ = false;
  state_.store(::alyrn::detail::backend::LoopState::kStopped, std::memory_order_release);
}

void Loop::RequestStop() noexcept {
  ::alyrn::detail::backend::LoopState observed = state_.load(std::memory_order_acquire);
  while (observed == ::alyrn::detail::backend::LoopState::kCreated || observed == ::alyrn::detail::backend::LoopState::kRunning) {
    if (state_.compare_exchange_weak(observed, ::alyrn::detail::backend::LoopState::kStopping,
                                     std::memory_order_acq_rel, std::memory_order_acquire)) {
      Wakeup();
      return;
    }
  }
}

void Loop::RunOnOwner(Functor callback) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::RunOnOwner called from wrong thread");
  callback();
}

void Loop::Schedule(coro::Work* work) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::Schedule called from wrong thread");
  ALYRN_CHECK(work != nullptr, "Loop::Schedule received null work");
  ALYRN_CHECK(pending_work_.PushBack(work), "Loop::Schedule received work already in a queue");
}

void Loop::RunPending() {
  ALYRN_CHECK(IsInLoopThread(), "Loop::RunPending called from wrong thread");
  while (HasImmediateWork()) {
    DoPendingWork();
  }
}

void Loop::UpdateChannel(Channel* channel) {
  ALYRN_CHECK(IsInLoopThread(), "Loop::UpdateChannel called from wrong thread");
  poller_->UpdateChannel(channel);
}

void Loop::RemoveChannel(Channel* channel) {
  ALYRN_CHECK(IsInLoopThread(), "Loop::RemoveChannel called from wrong thread");
  poller_->RemoveChannel(channel);
}

bool Loop::HasChannel(Channel* channel) const {
  ALYRN_CHECK(IsInLoopThread(), "Loop::HasChannel called from wrong thread");
  return poller_->HasChannel(channel);
}

void Loop::RegisterShutdownParticipant(LoopShutdownParticipant& participant) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::RegisterShutdownParticipant called from wrong thread");
  ALYRN_CHECK(shutdown_registry_.Register(&participant),
                 "Loop shutdown participant registered twice");
}

void Loop::UnregisterShutdownParticipant(LoopShutdownParticipant& participant) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::UnregisterShutdownParticipant called from wrong thread");
  ALYRN_CHECK(shutdown_registry_.Unregister(&participant),
                 "Loop shutdown participant was not registered");
}

bool Loop::IsInLoopThread() const noexcept {
  // The constructor enforces one Epoll Loop per thread, so the TLS owner
  // pointer is an exact owner-thread test and avoids pthread/self-id work on
  // the hot path. Keep the identity comparison for foreign callers.
  return t_loop_in_this_thread == this || thread_id_ == ::alyrn::detail::CurrentThreadId();
}

void Loop::DoPendingWork() {
  if (pending_work_.Empty()) {
    return;
  }
  coro::WorkQueue work;
  work.Splice(pending_work_);
  RunBatch(work);
}

void Loop::BeginShutdown() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::BeginShutdown called from wrong thread");
  if (shutdown_started_) {
    return;
  }
  shutdown_started_ = true;
  shutdown_registry_.RequestStop();
}

void Loop::DispatchWakeup(void* context) noexcept { static_cast<Loop*>(context)->DrainWakeup(); }

void Loop::DrainWakeup() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::DrainWakeup called from wrong thread");
  std::uint64_t value = 0;
  for (;;) {
    const ssize_t read = ::read(wakeup_fd_, &value, sizeof(value));
    if (read == static_cast<ssize_t>(sizeof(value))) {
      continue;
    }
    if (read < 0 && errno == EINTR) {
      continue;
    }
    ALYRN_CHECK(read < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
                   "Loop wakeup fd read failed");
    return;
  }
}

void Loop::Wakeup() noexcept {
  const std::uint64_t one = 1;
  for (;;) {
    const ssize_t written = ::write(wakeup_fd_, &one, sizeof(one));
    if (written == static_cast<ssize_t>(sizeof(one))) {
      return;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    ALYRN_CHECK(written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
                   "Loop wakeup fd write failed");
    return;
  }
}

void Loop::DetachWakeupChannel() noexcept {
  if (wakeup_channel_ == nullptr) {
    return;
  }
  if (!wakeup_channel_->IsNoneEvent()) {
    wakeup_channel_->DisableAll();
  }
  if (wakeup_channel_->IsRegistered()) {
    wakeup_channel_->Remove();
  }
  wakeup_channel_.reset();
}

bool Loop::HasImmediateWork() const {
  ALYRN_CHECK(IsInLoopThread(), "Loop::HasImmediateWork called from wrong thread");
  return !pending_work_.Empty();
}

time::TimerId Loop::RunAt(time::Deadline deadline, Functor callback) {
  ALYRN_CHECK(IsInLoopThread(), "Loop::RunAt called from wrong thread");
  return timer_queue_->AddTimer(std::move(callback), deadline, time::Duration::zero());
}

time::TimerId Loop::RunAfter(time::Duration delay, Functor callback) {
  ALYRN_CHECK(IsInLoopThread(), "Loop::RunAfter called from wrong thread");
  return timer_queue_->AddTimer(std::move(callback), time::SteadyNow() + delay,
                                time::Duration::zero());
}

time::TimerId Loop::RunEvery(time::Duration interval, Functor callback) {
  ALYRN_CHECK(IsInLoopThread(), "Loop::RunEvery called from wrong thread");
  return timer_queue_->AddTimer(std::move(callback), time::SteadyNow() + interval, interval);
}

void Loop::Cancel(time::TimerId id) {
  ALYRN_CHECK(IsInLoopThread(), "Loop::Cancel called from wrong thread");
  timer_queue_->Cancel(id);
}

}  // namespace alyrn::epoll
