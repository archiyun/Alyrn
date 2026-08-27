// SPDX-License-Identifier: MIT
#include "alyrn/reactor/detail/channel.h"

#include "alyrn/base/check.h"
#include "alyrn/reactor/loop.h"

namespace alyrn::reactor::detail {

Channel::Channel(Loop* loop, int fd) noexcept : loop_(loop), fd_(fd) {
  ALYRN_CHECK(loop_ != nullptr, "Channel: loop must not be null");
  ALYRN_CHECK(fd_ >= 0, "Channel: fd must be a valid non-negative descriptor");
}

Channel::Channel(Channel&& other) noexcept {
  ALYRN_CHECK(other.index_ == -1,
                 "Channel move requires the source to be detached from the Poller");

  loop_ = std::exchange(other.loop_, nullptr);
  fd_ = std::exchange(other.fd_, -1);
  events_ = std::exchange(other.events_, kNoneEvent);
  revents_ = std::exchange(other.revents_, kNoneEvent);
  index_ = std::exchange(other.index_, -1);
  trigger_mode_ = std::exchange(other.trigger_mode_, TriggerMode::kLevelTriggered);
  read_callback_ = std::exchange(other.read_callback_, nullptr);
  write_callback_ = std::exchange(other.write_callback_, nullptr);
  close_callback_ = std::exchange(other.close_callback_, nullptr);
  error_callback_ = std::exchange(other.error_callback_, nullptr);
  read_context_ = std::exchange(other.read_context_, nullptr);
  write_context_ = std::exchange(other.write_context_, nullptr);
  close_context_ = std::exchange(other.close_context_, nullptr);
  error_context_ = std::exchange(other.error_context_, nullptr);
}

Channel& Channel::operator=(Channel&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  ALYRN_CHECK(index_ == -1,
                 "Channel move assignment requires the destination to be detached from the Poller");
  ALYRN_CHECK(other.index_ == -1,
                 "Channel move requires the source to be detached from the Poller");

  loop_ = std::exchange(other.loop_, nullptr);
  fd_ = std::exchange(other.fd_, -1);
  events_ = std::exchange(other.events_, kNoneEvent);
  revents_ = std::exchange(other.revents_, kNoneEvent);
  index_ = std::exchange(other.index_, -1);
  trigger_mode_ = std::exchange(other.trigger_mode_, TriggerMode::kLevelTriggered);
  read_callback_ = std::exchange(other.read_callback_, nullptr);
  write_callback_ = std::exchange(other.write_callback_, nullptr);
  close_callback_ = std::exchange(other.close_callback_, nullptr);
  error_callback_ = std::exchange(other.error_callback_, nullptr);
  read_context_ = std::exchange(other.read_context_, nullptr);
  write_context_ = std::exchange(other.write_context_, nullptr);
  close_context_ = std::exchange(other.close_context_, nullptr);
  error_context_ = std::exchange(other.error_context_, nullptr);
  return *this;
}

void Channel::Update() noexcept {
  ALYRN_CHECK(loop_->IsInLoopThread(), "Channel::Update called from wrong thread");
  loop_->UpdateChannel(this);
}

void Channel::Remove() noexcept {
  ALYRN_CHECK(loop_->IsInLoopThread(), "Channel::Remove called from wrong thread");
  ALYRN_CHECK(IsNoneEvent(), "Channel::Remove called while events are still enabled");
  loop_->RemoveChannel(this);
}

bool Channel::IsRegistered() const noexcept { return loop_->HasChannel(const_cast<Channel*>(this)); }

void Channel::HandleEvent() noexcept {
  ALYRN_CHECK(loop_->IsInLoopThread(), "Channel::HandleEvent called from wrong thread");
  HandleEventUnchecked();
}

void Channel::HandleEventUnchecked() noexcept {
  // Channel callbacks are non-owning. Owners must detach the Channel before
  // destruction; keeping that lifetime rule explicit avoids a per-event
  // shared-owner lock on the loop-affine Reactor path.
  if (static_cast<bool>((revents_ & kHupEvent)) && !static_cast<bool>((revents_ & kReadEvent))) {
    if (close_callback_ != nullptr) {
      close_callback_(close_context_);
    }
  }

  if (static_cast<bool>(revents_ & kErrorEvent)) {
    if (error_callback_ != nullptr) {
      error_callback_(error_context_);
    }
  }

  if (static_cast<bool>(revents_ & kReadEvent)) {
    if (read_callback_ != nullptr) {
      read_callback_(read_context_);
    }
  }

  if (static_cast<bool>(revents_ & kWriteEvent)) {
    if (write_callback_ != nullptr) {
      write_callback_(write_context_);
    }
  }
}

}  // namespace alyrn::reactor::detail
