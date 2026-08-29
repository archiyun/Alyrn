// SPDX-License-Identifier: MIT
#include "alyrn/kqueue/loop.h"

#include <unistd.h>

#include <cerrno>
#include <mutex>
#include <utility>
#include <vector>

#include "alyrn/detail/check.h"
#include "alyrn/coro/frame_allocator.h"
#include "alyrn/detail/kqueue/channel.h"
#include "alyrn/detail/kqueue/poller.h"
#include "alyrn/detail/kqueue/timer_queue.h"
#include "alyrn/detail/net/socket.h"
#include "alyrn/time/timer_id.h"

namespace alyrn::kqueue {

namespace {

constexpr int kPollTimeMs = 10000;
thread_local Loop* t_loop_in_this_thread = nullptr;

}  // namespace

Loop::Loop(std::pmr::memory_resource* frame_resource)
    : Scheduler(frame_resource), poller_(std::make_unique<detail::Poller>()) {
  ALYRN_CHECK(t_loop_in_this_thread == nullptr,
                 "Loop: only one Loop may exist per thread");
  t_loop_in_this_thread = this;

  // Channel registration goes through IsInLoopThread(), so the TLS owner
  // pointer must be published before TimerQueue or the wakeup Channel.
  timer_queue_ = std::make_unique<detail::TimerQueue>(*poller_);

  int wakeup_fds[2] = {-1, -1};
  ALYRN_CHECK(::pipe(wakeup_fds) == 0, "Loop: wakeup pipe creation failed");
  wakeup_read_fd_ = wakeup_fds[0];
  wakeup_write_fd_ = wakeup_fds[1];

  /* pipe() cannot take flags portably, so both ends are configured after the
   * fact. A blocking write end would stall RequestStop() once the pipe fills
   * with coalesced wakeups that the loop has not drained yet. */
  for (const int fd : wakeup_fds) {
    ALYRN_CHECK(net::SetNonBlocking(fd).has_value(),
                   "Loop: failed to set wakeup pipe non-blocking");
    ALYRN_CHECK(net::SetCloseOnExec(fd).has_value(),
                   "Loop: failed to set wakeup pipe close-on-exec");
  }

  wakeup_channel_ = std::make_unique<Channel>(this, wakeup_read_fd_);
  wakeup_channel_->SetReadCallback(&Loop::DispatchWakeup, this);
  wakeup_channel_->EnableReading();
}

Loop::~Loop() {
  ALYRN_CHECK(IsInLoopThread(), "Loop destructor called from wrong thread");
  ALYRN_CHECK(!looping_, "Loop destroyed while looping");

  ALYRN_CHECK(pending_work_.Empty(), "Loop destroyed with pending owner work");
  ALYRN_CHECK(shutdown_registry_.Empty(),
                 "Loop destroyed with registered shutdown resources");
  // TimerQueue and the wakeup Channel unregister while TLS still names this
  // Loop; the member unique_ptrs would otherwise run after this pointer is
  // cleared.
  timer_queue_.reset();
  DetachWakeupChannel();
  {
    std::lock_guard lock{posted_mutex_};
    ALYRN_CHECK(posted_.empty(), "Loop destroyed with pending posted callbacks");
    if (wakeup_write_fd_ >= 0) {
      ::close(wakeup_write_fd_);
      wakeup_write_fd_ = -1;
    }
  }
  if (wakeup_read_fd_ >= 0) {
    ::close(wakeup_read_fd_);
  }
  t_loop_in_this_thread = nullptr;
}

void Loop::Run(std::stop_token token) {
  ALYRN_CHECK(IsInLoopThread(), "Loop::Run called from wrong thread");
  ALYRN_CHECK(!looping_, "Loop::Run called while already running");

  auto expected = backend::LoopState::kCreated;
  if (!state_.compare_exchange_strong(expected, backend::LoopState::kRunning,
                                      std::memory_order_acq_rel, std::memory_order_acquire)) {
    ALYRN_CHECK(expected == backend::LoopState::kStopping,
                   "Loop::Run may only run a created or stopping loop");
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
    coro::CoroFramePoolResource::DrainCurrent();
  }

  BeginShutdown();
  RunPending();
  coro::CoroFramePoolResource::DrainCurrent();
  looping_ = false;
  state_.store(backend::LoopState::kStopped, std::memory_order_release);
}

void Loop::RequestStop() noexcept {
  backend::LoopState observed = state_.load(std::memory_order_acquire);
  while (observed == backend::LoopState::kCreated || observed == backend::LoopState::kRunning) {
    if (state_.compare_exchange_weak(observed, backend::LoopState::kStopping,
                                     std::memory_order_acq_rel, std::memory_order_acquire)) {
      Wakeup();
      return;
    }
  }
}

void Loop::RunOnOwner(Functor callback) {
  ALYRN_CHECK(IsInLoopThread(), "Loop::RunOnOwner called from wrong thread");
  callback();
}

void Loop::Post(Functor callback) {
  /* Wakeup stays under the lock. Otherwise a 0-timeout poll can drain this
   * callback, RequestStop, and close the pipe before the write returns. */
  std::lock_guard lock{posted_mutex_};
  posted_.push_back(std::move(callback));
  Wakeup();
}

void Loop::Schedule(coro::Work* work) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::Schedule called from wrong thread");
  ALYRN_CHECK(work != nullptr, "Loop::Schedule received null work");
  ALYRN_CHECK(pending_work_.PushBack(work),
                 "Loop::Schedule received work already in a queue");
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
  ALYRN_CHECK(IsInLoopThread(),
                 "Loop::RegisterShutdownParticipant called from wrong thread");
  ALYRN_CHECK(shutdown_registry_.Register(&participant),
                 "Loop shutdown participant registered twice");
}

void Loop::UnregisterShutdownParticipant(LoopShutdownParticipant& participant) noexcept {
  ALYRN_CHECK(IsInLoopThread(),
                 "Loop::UnregisterShutdownParticipant called from wrong thread");
  ALYRN_CHECK(shutdown_registry_.Unregister(&participant),
                 "Loop shutdown participant was not registered");
}

bool Loop::IsInLoopThread() const noexcept {
  return t_loop_in_this_thread == this;
}

void Loop::DoPendingWork() {
  DrainPostedWork();
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

void Loop::DrainPostedWork() {
  std::vector<Functor> posted;
  {
    std::lock_guard lock{posted_mutex_};
    posted.swap(posted_);
  }
  for (Functor& callback : posted) {
    callback();
  }
}

void Loop::BeginShutdown() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::BeginShutdown called from wrong thread");
  if (shutdown_started_) {
    return;
  }
  shutdown_started_ = true;
  shutdown_registry_.RequestStop();
}

void Loop::DispatchWakeup(void* context) noexcept {
  static_cast<Loop*>(context)->DrainWakeup();
}

void Loop::DrainWakeup() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::DrainWakeup called from wrong thread");
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
    ALYRN_CHECK(bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
                   "Loop wakeup pipe read failed");
    return;
  }
}

void Loop::Wakeup() noexcept {
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
    ALYRN_CHECK(written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
                   "Loop wakeup pipe write failed");
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
  if (!pending_work_.Empty()) {
    return true;
  }
  std::lock_guard lock{posted_mutex_};
  return !posted_.empty();
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

}  // namespace alyrn::kqueue
