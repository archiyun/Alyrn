// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/net/channel.h"

#include "coropact/base/check.h"
#include "coropact/net/event_loop.h"

namespace coropact::net {

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop), fd_(fd), events_(0), revents_(0), index_(-1), tied_(false) {
  COROPACT_DCHECK(loop_ != nullptr, "Channel: loop must not be null");
  COROPACT_DCHECK(fd_ >= 0, "Channel: fd must be a valid non-negative descriptor");
}

Channel::Channel(Channel&& other) noexcept
    : loop_(nullptr),
      fd_(-1),
      events_(kNoneEvent),
      revents_(kNoneEvent),
      index_(-1),
      trigger_mode_(TriggerMode::kLevelTriggered),
      tied_(false) {
  COROPACT_CHECK(other.index_ == -1, "Channel move requires the source to be detached from the Poller");

  loop_ = std::exchange(other.loop_, nullptr);
  fd_ = std::exchange(other.fd_, -1);
  events_ = std::exchange(other.events_, kNoneEvent);
  revents_ = std::exchange(other.revents_, kNoneEvent);
  index_ = std::exchange(other.index_, -1);
  trigger_mode_ = std::exchange(other.trigger_mode_, TriggerMode::kLevelTriggered);
  tie_ = std::move(other.tie_);
  tied_ = std::exchange(other.tied_, false);
  read_callback_ = std::move(other.read_callback_);
  write_callback_ = std::move(other.write_callback_);
  close_callback_ = std::move(other.close_callback_);
  error_callback_ = std::move(other.error_callback_);
}

Channel& Channel::operator=(Channel&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  COROPACT_CHECK(index_ == -1,
             "Channel move assignment requires the destination to be detached from the Poller");
  COROPACT_CHECK(other.index_ == -1, "Channel move requires the source to be detached from the Poller");

  loop_ = std::exchange(other.loop_, nullptr);
  fd_ = std::exchange(other.fd_, -1);
  events_ = std::exchange(other.events_, kNoneEvent);
  revents_ = std::exchange(other.revents_, kNoneEvent);
  index_ = std::exchange(other.index_, -1);
  trigger_mode_ = std::exchange(other.trigger_mode_, TriggerMode::kLevelTriggered);
  tie_ = std::move(other.tie_);
  tied_ = std::exchange(other.tied_, false);
  read_callback_ = std::move(other.read_callback_);
  write_callback_ = std::move(other.write_callback_);
  close_callback_ = std::move(other.close_callback_);
  error_callback_ = std::move(other.error_callback_);
  return *this;
}

void Channel::Tie(const std::shared_ptr<void>& obj) {
  COROPACT_DCHECK(!tied_, "Channel::Tie called more than once");
  tie_ = obj;
  tied_ = true;
}

void Channel::Update() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "Channel::Update called from wrong thread");
  loop_->UpdateChannel(this);
}

void Channel::Remove() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "Channel::Remove called from wrong thread");
  COROPACT_DCHECK(IsNoneEvent(), "Channel::Remove called while events are still enabled");
  loop_->RemoveChannel(this);
}

void Channel::HandleEvent(coropact::time::Timestamp receive_time) {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "Channel::HandleEvent called from wrong thread");
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

void Channel::HandleEventWithGuard(coropact::time::Timestamp receive_time) {
  // kHupEvent without kReadEvent usually means the peer has closed the connection
  // and there is no more readable data left in the socket buffer.
  if (static_cast<bool>((revents_ & kHupEvent)) && !static_cast<bool>((revents_ & kReadEvent))) {
    if (close_callback_) close_callback_();
  }

  if (static_cast<bool>(revents_ & kErrorEvent)) {
    if (error_callback_) error_callback_();
  }

  if (static_cast<bool>(revents_ & kReadEvent)) {
    if (read_callback_) read_callback_(receive_time);
  }

  if (static_cast<bool>(revents_ & kWriteEvent)) {
    if (write_callback_) write_callback_();
  }
}

}  // namespace coropact::net
