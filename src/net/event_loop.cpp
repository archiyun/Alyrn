#include "runtime/time/timestamp.h"
#include "runtime/log/logger.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/channel.h"
#include "runtime/net/poller.h"
#include "runtime/net/timer_id.h"
#include "runtime/net/timer_queue.h"

#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <sys/eventfd.h>
#include <unistd.h>

namespace runtime::net {

class TimerId;

namespace {

static constexpr int kPollTimeMs = 10000;

int CreateEventfd() {
  const int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (evtfd < 0) {
    LOG_FATAL() << "eventfd creation failed: errno=" << errno
                << " message=" << std::strerror(errno);
    std::abort();
  }
  return evtfd;
}

void WriteEventfd(int fd) {
  const uint64_t one = 1;
  while (true) {
    const ssize_t n = ::write(fd, &one, sizeof(one));
    if (n == static_cast<ssize_t>(sizeof(one))) {
      return;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n < 0 && errno == EAGAIN) {
      return;
    }

    LOG_ERROR() << "eventfd write failed: fd=" << fd
                << " errno=" << errno
                << " message=" << std::strerror(errno);
    return;
  }
}

void ReadEventfd(int fd) {
  uint64_t one = 0;
  while (true) {
    const ssize_t n = ::read(fd, &one, sizeof(one));
    if (n == static_cast<ssize_t>(sizeof(one))) {
      return;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n < 0 && errno == EAGAIN) {
      return;
    }

    LOG_ERROR() << "eventfd read failed: fd=" << fd
                << " errno=" << errno
                << " message=" << std::strerror(errno);
    return;
  }
}

thread_local EventLoop* t_loop_in_this_thread = nullptr;

}  // namespace

EventLoop::EventLoop()
    : looping_(false),
      quit_(false),
      calling_pending_functors_(false),
      thread_id_(std::this_thread::get_id()),
      poller_(Poller::NewDefaultPoller(this)),
      wakeup_fd_(CreateEventfd()),
      wakeup_channel_(std::make_unique<Channel>(this, wakeup_fd_)),
      timer_queue_(std::make_unique<TimerQueue>(this)) {
  assert(t_loop_in_this_thread == nullptr);
  t_loop_in_this_thread = this;

  // The wakeup fd is monitored like a normal Channel so other threads can
  // interrupt epoll_wait when they queue work into this loop.
  wakeup_channel_->SetReadCallback(
      [this](runtime::time::Timestamp) { HandleRead(); });
  wakeup_channel_->EnableReading();

  LOG_DEBUG() << "event loop created: wakeup_fd=" << wakeup_fd_;
}

EventLoop::~EventLoop() {
  assert(IsInLoopThread());
  assert(!looping_);

  wakeup_channel_->DisableAll();
  wakeup_channel_->Remove();
  ::close(wakeup_fd_);
  t_loop_in_this_thread = nullptr;
}

void EventLoop::Loop() {
  assert(IsInLoopThread());
  assert(!looping_);
  looping_ = true;
  quit_ = false;

  LOG_INFO() << "event loop entering loop";

  while (!quit_) {
    active_channels_.clear();
    poll_return_time_ = poller_->Poll(kPollTimeMs, &active_channels_);

    for (Channel* channel : active_channels_) {
      channel->HandleEvent(poll_return_time_);
    }

    DoPendingFunctors();
  }

  looping_ = false;
  LOG_INFO() << "event loop exited loop";
}

void EventLoop::Quit() {
  quit_ = true;

  // If Quit() is called from another thread, wake up the loop so it can observe
  // the updated quit flag instead of staying blocked in epoll_wait.
  if (!IsInLoopThread()) {
    Wakeup();
  }
}

void EventLoop::RunInLoop(Functor cb) {
  if (IsInLoopThread()) {
    cb();
  } else {
    QueueInLoop(std::move(cb));
  }
}

void EventLoop::QueueInLoop(Functor cb) {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    pending_functors_.push_back(std::move(cb));
  }

  // Wake the loop when work is queued from another thread, or when the loop is
  // already executing pending functors and needs to observe newly queued work
  // in a later iteration.
  if (!IsInLoopThread() || calling_pending_functors_.load()) {
    Wakeup();
  }
}

void EventLoop::UpdateChannel(Channel* channel) {
  assert(IsInLoopThread());
  poller_->UpdateChannel(channel);
}

void EventLoop::RemoveChannel(Channel* channel) {
  assert(IsInLoopThread());
  poller_->RemoveChannel(channel);
}

bool EventLoop::HasChannel(Channel* channel) const {
  assert(IsInLoopThread());
  return poller_->HasChannel(channel);
}

bool EventLoop::IsInLoopThread() const {
  return thread_id_ == std::this_thread::get_id();
}

void EventLoop::Wakeup() {
  WriteEventfd(wakeup_fd_);
}

void EventLoop::HandleRead() {
  ReadEventfd(wakeup_fd_);
}

void EventLoop::DoPendingFunctors() {
  std::vector<Functor> functors;
  calling_pending_functors_ = true;

  {
    std::lock_guard<std::mutex> lk(mutex_);
    functors.swap(pending_functors_);
  }

  // Move the pending queue into a local vector before running callbacks so
  // producers can continue to enqueue work without holding the mutex during
  // callback execution.
  for (auto& functor : functors) {
    functor();
  }

  calling_pending_functors_ = false;
}

TimerId EventLoop::RunAt(runtime::time::Timestamp time, Functor cb) {
  return timer_queue_->AddTimer(std::move(cb), time, 0.0);
}

TimerId EventLoop::RunAfter(double delay, Functor cb) {
  using runtime::time::AddTime;
  using runtime::time::Timestamp;
  return timer_queue_->AddTimer(
      std::move(cb), AddTime(Timestamp::Now(), delay), 0.0);
}

TimerId EventLoop::RunEvery(double interval, Functor cb) {
  using runtime::time::AddTime;
  using runtime::time::Timestamp;
  return timer_queue_->AddTimer(
      std::move(cb), AddTime(Timestamp::Now(), interval), interval);
}

void EventLoop::Cancel(TimerId id) {
  timer_queue_->Cancel(id);
}

}  // namespace runtime::net
