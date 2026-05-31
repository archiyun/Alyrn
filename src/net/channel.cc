// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/net/channel.h"

#include "runtime/net/event_loop.h"
#include "runtime/net/net_assert.h"

namespace runtime::net {

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop), fd_(fd), events_(0), revents_(0), index_(-1), tied_(false) {
  RUNTIME_ASSERT(loop_ != nullptr, "Channel: loop must not be null");
  RUNTIME_ASSERT(fd_ >= 0, "Channel: fd must be a valid non-negative descriptor");
}

Channel::~Channel() = default;

void Channel::Tie(const std::shared_ptr<void>& obj) {
  RUNTIME_ASSERT(!tied_, "Channel::Tie called more than once");
  tie_ = obj;
  tied_ = true;
}

void Channel::Update() {
  RUNTIME_ASSERT(loop_->IsInLoopThread(), "Channel::Update called from wrong thread");
  loop_->UpdateChannel(this);
}

void Channel::Remove() {
  RUNTIME_ASSERT(loop_->IsInLoopThread(), "Channel::Remove called from wrong thread");
  RUNTIME_ASSERT(IsNoneEvent(), "Channel::Remove called while events are still enabled");
  loop_->RemoveChannel(this);
}

void Channel::HandleEvent(runtime::time::Timestamp receive_time) {
  RUNTIME_ASSERT(loop_->IsInLoopThread(), "Channel::HandleEvent called from wrong thread");
  if (tied_) {
    // Hold a temporary shared reference while dispatching callbacks so the
    // owner cannot be destroyed in the middle of event handling.
    std::shared_ptr<void> guard = tie_.lock();
    if (guard) {
      HandleEventWithGuard(receive_time);
    }
  } else {
    HandleEventWithGuard(receive_time);
  }
}

void Channel::HandleEventWithGuard(runtime::time::Timestamp receive_time) {
  // kHupEvent without kReadEvent usually means the peer has closed the connection
  // and there is no more readable data left in the socket buffer.
  if ((revents_ & kHupEvent) && !(revents_ & kReadEvent)) {
    if (close_callback_) close_callback_();
  }

  if (revents_ & kErrorEvent) {
    if (error_callback_) error_callback_();
  }

  if (revents_ & kReadEvent) {
    if (read_callback_) read_callback_(receive_time);
  }

  if (revents_ & kWriteEvent) {
    if (write_callback_) write_callback_();
  }
}

}  // namespace runtime::net
