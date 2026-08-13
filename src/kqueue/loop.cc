// SPDX-License-Identifier: MIT
#include "coropact/kqueue/loop.h"

#include <unistd.h>

#include <cerrno>

#include "coropact/base/check.h"
#include "coropact/base/current_thread.h"
#include "coropact/kqueue/detail/channel.h"
#include "coropact/kqueue/detail/kqueue_poller.h"
#include "coropact/kqueue/detail/timer_queue.h"
#include "coropact/net/socket.h"
#include "coropact/time/timer_id.h"

namespace coropact::kqueue {

namespace {

constexpr int kPollTimeMs = 10000;
thread_local KqueueLoop* t_loop_in_this_thread = nullptr;

}  // namespace

KqueueLoop::KqueueLoop(std::pmr::memory_resource* frame_resource)
    : Scheduler(frame_resource),
      thread_id_(base::CurrentThreadId()),
      poller_(std::make_unique<detail::KqueuePoller>()),
      timer_queue_(std::make_unique<detail::TimerQueue>(*poller_)) {
  COROPACT_CHECK(t_loop_in_this_thread == nullptr,
                 "KqueueLoop: only one KqueueLoop may exist per thread");
  t_loop_in_this_thread = this;

  int wakeup_fds[2] = {-1, -1};
  COROPACT_CHECK(::pipe(wakeup_fds) == 0, "KqueueLoop: wakeup pipe creation failed");
  wakeup_read_fd_ = wakeup_fds[0];
  wakeup_write_fd_ = wakeup_fds[1];

  /* pipe() cannot take flags portably, so both ends are configured after the
   * fact. A blocking write end would stall RequestStop() once the pipe fills
   * with coalesced wakeups that the loop has not drained yet. */
  for (const int fd : wakeup_fds) {
    COROPACT_CHECK(net::SetNonBlocking(fd).has_value(),
                   "KqueueLoop: failed to set wakeup pipe non-blocking");
    COROPACT_CHECK(net::SetCloseOnExec(fd).has_value(),
                   "KqueueLoop: failed to set wakeup pipe close-on-exec");
  }

  wakeup_channel_ = std::make_unique<Channel>(this, wakeup_read_fd_);
  wakeup_channel_->SetReadCallback(&KqueueLoop::DispatchWakeup, this);
  wakeup_channel_->EnableReading();
}

KqueueLoop::~KqueueLoop() {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop destructor called from wrong thread");
  COROPACT_CHECK(!looping_, "KqueueLoop destroyed while looping");

  COROPACT_CHECK(pending_work_.Empty(), "KqueueLoop destroyed with pending owner work");
  COROPACT_CHECK(shutdown_registry_.Empty(),
                 "KqueueLoop destroyed with registered shutdown resources");
  DetachWakeupChannel();
  if (wakeup_write_fd_ >= 0) {
    ::close(wakeup_write_fd_);
  }
  if (wakeup_read_fd_ >= 0) {
    ::close(wakeup_read_fd_);
  }
  t_loop_in_this_thread = nullptr;
}

void KqueueLoop::Run(std::stop_token token) {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::Run called from wrong thread");
  COROPACT_CHECK(!looping_, "KqueueLoop::Run called while already running");

  backend::LoopState expected = backend::LoopState::kCreated;
  if (!state_.compare_exchange_strong(expected, backend::LoopState::kRunning,
                                      std::memory_order_acq_rel, std::memory_order_acquire)) {
    COROPACT_CHECK(expected == backend::LoopState::kStopping,
                   "KqueueLoop::Run may only run a created or stopping loop");
  }

  looping_ = true;
  std::stop_callback on_stop{token, [this] { RequestStop(); }};

  while (State() == backend::LoopState::kRunning) {
    DoPendingWork();

    /* Work run above may have requested stop. Re-checking here keeps a final
     * queued continuation from being stranded behind another blocking wait. */
    if (State() != backend::LoopState::kRunning) {
      break;
    }

    active_channels_.clear();

    /* Polling with a zero timeout while work is already queued is what stops a
     * coroutine that resumed another coroutine from waiting on an unrelated
     * readiness event. */
    const int timeout_ms = HasImmediateWork() ? 0 : kPollTimeMs;
    poller_->Poll(timeout_ms, &active_channels_);

    for (Channel* channel : active_channels_) {
      channel->HandleEvent();
    }
    poller_->DispatchTimerExpire();
  }

  BeginShutdown();
  RunPending();
  looping_ = false;
  state_.store(backend::LoopState::kStopped, std::memory_order_release);
}

void KqueueLoop::RequestStop() noexcept {
  backend::LoopState observed = state_.load(std::memory_order_acquire);
  while (observed == backend::LoopState::kCreated || observed == backend::LoopState::kRunning) {
    if (state_.compare_exchange_weak(observed, backend::LoopState::kStopping,
                                     std::memory_order_acq_rel, std::memory_order_acquire)) {
      Wakeup();
      return;
    }
  }
}

void KqueueLoop::RunOnOwner(Functor callback) {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::RunOnOwner called from wrong thread");
  callback();
}

void KqueueLoop::Schedule(coro::Work* work) noexcept {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::Schedule called from wrong thread");
  COROPACT_CHECK(work != nullptr, "KqueueLoop::Schedule received null work");
  COROPACT_CHECK(pending_work_.PushBack(work),
                 "KqueueLoop::Schedule received work already in a queue");
}

void KqueueLoop::RunPending() {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::RunPending called from wrong thread");
  while (HasImmediateWork()) {
    DoPendingWork();
  }
}

void KqueueLoop::UpdateChannel(Channel* channel) {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::UpdateChannel called from wrong thread");
  poller_->UpdateChannel(channel);
}

void KqueueLoop::RemoveChannel(Channel* channel) {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::RemoveChannel called from wrong thread");
  poller_->RemoveChannel(channel);
}

bool KqueueLoop::HasChannel(Channel* channel) const {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::HasChannel called from wrong thread");
  return poller_->HasChannel(channel);
}

void KqueueLoop::RegisterShutdownParticipant(LoopShutdownParticipant& participant) noexcept {
  COROPACT_CHECK(IsInLoopThread(),
                 "KqueueLoop::RegisterShutdownParticipant called from wrong thread");
  COROPACT_CHECK(shutdown_registry_.Register(&participant),
                 "KqueueLoop shutdown participant registered twice");
}

void KqueueLoop::UnregisterShutdownParticipant(LoopShutdownParticipant& participant) noexcept {
  COROPACT_CHECK(IsInLoopThread(),
                 "KqueueLoop::UnregisterShutdownParticipant called from wrong thread");
  COROPACT_CHECK(shutdown_registry_.Unregister(&participant),
                 "KqueueLoop shutdown participant was not registered");
}

bool KqueueLoop::IsInLoopThread() const noexcept { return thread_id_ == base::CurrentThreadId(); }

void KqueueLoop::DoPendingWork() {
  if (pending_work_.Empty()) {
    return;
  }
  /* RunBatch() installs this loop as the current Scheduler and activates its
   * frame memory resource. Calling Work::Run() directly would allocate
   * coroutine frames from whatever resource the thread last had active. */
  coro::WorkQueue work;
  work.Splice(pending_work_);
  RunBatch(work);
}

void KqueueLoop::BeginShutdown() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::BeginShutdown called from wrong thread");
  if (shutdown_started_) {
    return;
  }
  shutdown_started_ = true;
  shutdown_registry_.RequestStop();
}

void KqueueLoop::DispatchWakeup(void* context) noexcept {
  static_cast<KqueueLoop*>(context)->DrainWakeup();
}

void KqueueLoop::DrainWakeup() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::DrainWakeup called from wrong thread");
  /* Wakeups coalesce: any number of pending bytes means the same thing, so the
   * pipe is drained to empty rather than read one request at a time. */
  char buffer[64];
  for (;;) {
    const ssize_t bytes = ::read(wakeup_read_fd_, buffer, sizeof(buffer));
    if (bytes > 0) {
      continue;
    }
    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    COROPACT_CHECK(bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
                   "KqueueLoop wakeup pipe read failed");
    return;
  }
}

void KqueueLoop::Wakeup() noexcept {
  const char one = 'w';
  for (;;) {
    const ssize_t written = ::write(wakeup_write_fd_, &one, sizeof(one));
    if (written == static_cast<ssize_t>(sizeof(one))) {
      return;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    /* A full pipe already carries an undrained wakeup, so dropping this one
     * loses nothing. */
    COROPACT_CHECK(written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
                   "KqueueLoop wakeup pipe write failed");
    return;
  }
}

void KqueueLoop::DetachWakeupChannel() noexcept {
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

bool KqueueLoop::HasImmediateWork() const {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::HasImmediateWork called from wrong thread");
  return !pending_work_.Empty();
}

time::TimerId KqueueLoop::RunAt(time::Deadline deadline, Functor callback) {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::RunAt called from wrong thread");
  return timer_queue_->AddTimer(std::move(callback), deadline, time::Duration::zero());
}

time::TimerId KqueueLoop::RunAfter(time::Duration delay, Functor callback) {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::RunAfter called from wrong thread");
  return timer_queue_->AddTimer(std::move(callback), time::SteadyNow() + delay,
                                time::Duration::zero());
}

time::TimerId KqueueLoop::RunEvery(time::Duration interval, Functor callback) {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::RunEvery called from wrong thread");
  return timer_queue_->AddTimer(std::move(callback), time::SteadyNow() + interval, interval);
}

void KqueueLoop::Cancel(time::TimerId id) {
  COROPACT_CHECK(IsInLoopThread(), "KqueueLoop::Cancel called from wrong thread");
  timer_queue_->Cancel(id);
}

}  // namespace coropact::kqueue
