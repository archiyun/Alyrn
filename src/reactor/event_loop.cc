// SPDX-License-Identifier: MIT
#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>

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

constexpr int kPollTimeMs = 10000;
thread_local EventLoop* t_loop_in_this_thread = nullptr;

}  // namespace

EventLoop::EventLoop(std::pmr::memory_resource* frame_resource)
    : Scheduler(frame_resource),
      thread_id_(base::CurrentThreadId()),
      poller_(Poller::NewDefaultPoller(this)),
      timer_queue_(std::make_unique<TimerQueue>(this)) {
  COROPACT_CHECK(t_loop_in_this_thread == nullptr,
                 "EventLoop: only one EventLoop may exist per thread");
  t_loop_in_this_thread = this;

  wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  COROPACT_CHECK(wakeup_fd_ >= 0, "EventLoop: eventfd creation failed");
  wakeup_channel_ = std::make_unique<Channel>(this, wakeup_fd_);
  wakeup_channel_->SetReadCallback(&EventLoop::DispatchWakeup, this);
  wakeup_channel_->EnableReading();
}

EventLoop::~EventLoop() {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop destructor called from wrong thread");
  COROPACT_CHECK(!looping_, "EventLoop destroyed while looping");

  COROPACT_CHECK(pending_work_.Empty(), "EventLoop destroyed with pending owner work");
  COROPACT_CHECK(shutdown_registry_.Empty(),
                 "EventLoop destroyed with registered shutdown resources");
  DetachWakeupChannel();
  if (wakeup_fd_ >= 0) {
    ::close(wakeup_fd_);
  }
  t_loop_in_this_thread = nullptr;
}

void EventLoop::Run(std::stop_token token) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::Run called from wrong thread");
  COROPACT_CHECK(!looping_, "EventLoop::Run called while already running");

  backend::LoopState expected = backend::LoopState::kCreated;
  if (!state_.compare_exchange_strong(expected, backend::LoopState::kRunning,
                                      std::memory_order_acq_rel, std::memory_order_acquire)) {
    COROPACT_CHECK(expected == backend::LoopState::kStopping,
                   "EventLoop::Run may only run a created or stopping loop");
  }

  looping_ = true;
  std::stop_callback on_stop{token, [this] { RequestStop(); }};

  while (State() == backend::LoopState::kRunning) {
    DoPendingWork();

    if (State() != backend::LoopState::kRunning) {
      break;
    }

    active_channels_.clear();

    const int timeout_ms = HasImmediateWork() ? 0 : kPollTimeMs;
    poller_->Poll(timeout_ms, &active_channels_);

    for (Channel* channel : active_channels_) {
      channel->HandleEvent();
    }
  }

  BeginShutdown();
  RunPending();
  looping_ = false;
  state_.store(backend::LoopState::kStopped, std::memory_order_release);
}

void EventLoop::RequestStop() noexcept {
  backend::LoopState observed = state_.load(std::memory_order_acquire);
  while (observed == backend::LoopState::kCreated || observed == backend::LoopState::kRunning) {
    if (state_.compare_exchange_weak(observed, backend::LoopState::kStopping,
                                     std::memory_order_acq_rel, std::memory_order_acquire)) {
      Wakeup();
      return;
    }
  }
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

void EventLoop::UpdateChannel(Channel* channel) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::UpdateChannel called from wrong thread");
  poller_->UpdateChannel(channel);
}

void EventLoop::RemoveChannel(Channel* channel) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::RemoveChannel called from wrong thread");
  poller_->RemoveChannel(channel);
}

bool EventLoop::HasChannel(Channel* channel) const {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::HasChannel called from wrong thread");
  return poller_->HasChannel(channel);
}

void EventLoop::RegisterShutdownParticipant(LoopShutdownParticipant& participant) noexcept {
  COROPACT_CHECK(IsInLoopThread(),
                 "EventLoop::RegisterShutdownParticipant called from wrong thread");
  COROPACT_CHECK(shutdown_registry_.Register(&participant),
                 "EventLoop shutdown participant registered twice");
}

void EventLoop::UnregisterShutdownParticipant(LoopShutdownParticipant& participant) noexcept {
  COROPACT_CHECK(IsInLoopThread(),
                 "EventLoop::UnregisterShutdownParticipant called from wrong thread");
  COROPACT_CHECK(shutdown_registry_.Unregister(&participant),
                 "EventLoop shutdown participant was not registered");
}

bool EventLoop::IsInLoopThread() const noexcept { return thread_id_ == base::CurrentThreadId(); }

void EventLoop::DoPendingWork() {
  if (pending_work_.Empty()) {
    return;
  }
  coro::WorkQueue work;
  work.Splice(pending_work_);
  RunBatch(work);
}

void EventLoop::BeginShutdown() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::BeginShutdown called from wrong thread");
  if (shutdown_started_) {
    return;
  }
  shutdown_started_ = true;
  shutdown_registry_.RequestStop();
}

void EventLoop::DispatchWakeup(void* context) noexcept {
  static_cast<EventLoop*>(context)->DrainWakeup();
}

void EventLoop::DrainWakeup() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::DrainWakeup called from wrong thread");
  std::uint64_t value = 0;
  for (;;) {
    const ssize_t read = ::read(wakeup_fd_, &value, sizeof(value));
    if (read == static_cast<ssize_t>(sizeof(value))) {
      continue;
    }
    if (read < 0 && errno == EINTR) {
      continue;
    }
    COROPACT_CHECK(read < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
                   "EventLoop wakeup fd read failed");
    return;
  }
}

void EventLoop::Wakeup() noexcept {
  const std::uint64_t one = 1;
  for (;;) {
    const ssize_t written = ::write(wakeup_fd_, &one, sizeof(one));
    if (written == static_cast<ssize_t>(sizeof(one))) {
      return;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    COROPACT_CHECK(written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
                   "EventLoop wakeup fd write failed");
    return;
  }
}

void EventLoop::DetachWakeupChannel() noexcept {
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

bool EventLoop::HasImmediateWork() const {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::HasImmediateWork called from wrong thread");
  return !pending_work_.Empty();
}

time::TimerId EventLoop::RunAt(time::Deadline deadline, Functor callback) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::RunAt called from wrong thread");
  return timer_queue_->AddTimer(std::move(callback), deadline, time::Duration::zero());
}

time::TimerId EventLoop::RunAfter(time::Duration delay, Functor callback) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::RunAfter called from wrong thread");
  return timer_queue_->AddTimer(std::move(callback), time::SteadyNow() + delay,
                                time::Duration::zero());
}

time::TimerId EventLoop::RunEvery(time::Duration interval, Functor callback) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::RunEvery called from wrong thread");
  return timer_queue_->AddTimer(std::move(callback), time::SteadyNow() + interval, interval);
}

void EventLoop::Cancel(time::TimerId id) {
  COROPACT_CHECK(IsInLoopThread(), "EventLoop::Cancel called from wrong thread");
  timer_queue_->Cancel(id);
}

}  // namespace coropact::reactor
