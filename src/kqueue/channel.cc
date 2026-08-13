// SPDX-License-Identifier: MIT
#include "coropact/kqueue/detail/channel.h"

#include <utility>

#include "coropact/base/check.h"
#include "coropact/kqueue/loop.h"

namespace coropact::kqueue::detail {

Channel::Channel(KqueueLoop* loop, int fd) noexcept : loop_(loop), fd_(fd) {
  COROPACT_CHECK(loop_ != nullptr, "Channel: loop must not be null");
  COROPACT_CHECK(fd_ >= 0, "Channel: fd must be a valid non-negative descriptor");
}

/*
 * A registered poller entry stores the Channel object's address, so a move
 * would leave the kqueue registration pointing at freed storage. Both sides
 * must therefore be detached first. Registration state lives in the poller,
 * not in the Channel, which is why this asks the loop instead of inspecting a
 * local index.
 */
Channel::Channel(Channel&& other) noexcept {
  COROPACT_CHECK(other.loop_ == nullptr || !other.IsRegistered(),
                 "Channel move requires the source to be detached from the Poller");

  loop_ = std::exchange(other.loop_, nullptr);
  fd_ = std::exchange(other.fd_, -1);
  events_ = std::exchange(other.events_, kNoneEvent);
  revents_ = std::exchange(other.revents_, kNoneEvent);
  active_epoch_ = std::exchange(other.active_epoch_, 0);
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

  COROPACT_CHECK(loop_ == nullptr || !IsRegistered(),
                 "Channel move assignment requires the destination to be detached from the Poller");
  COROPACT_CHECK(other.loop_ == nullptr || !other.IsRegistered(),
                 "Channel move requires the source to be detached from the Poller");

  loop_ = std::exchange(other.loop_, nullptr);
  fd_ = std::exchange(other.fd_, -1);
  events_ = std::exchange(other.events_, kNoneEvent);
  revents_ = std::exchange(other.revents_, kNoneEvent);
  active_epoch_ = std::exchange(other.active_epoch_, 0);
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

void Channel::Update() {
  COROPACT_CHECK(loop_->IsInLoopThread(), "Channel::Update called from wrong thread");
  loop_->UpdateChannel(this);
}

/*
 * Remove() requires an empty interest set because the poller submits EV_DELETE
 * per filter. Leaving a filter enabled here would drop a live kqueue
 * registration that still names this Channel.
 */
void Channel::Remove() {
  COROPACT_CHECK(loop_->IsInLoopThread(), "Channel::Remove called from wrong thread");
  COROPACT_CHECK(IsNoneEvent(), "Channel::Remove called while events are still enabled");
  loop_->RemoveChannel(this);
}

bool Channel::IsRegistered() const { return loop_->HasChannel(const_cast<Channel*>(this)); }

void Channel::HandleEvent() {
  COROPACT_CHECK(loop_->IsInLoopThread(), "Channel::HandleEvent called from wrong thread");
  /*
   * Channel callbacks are non-owning. Owners must detach the Channel before
   * destruction; keeping that lifetime rule explicit avoids a per-event
   * shared-owner lock on the loop-affine kqueue path.
   *
   * The close branch is deliberately conditional on the absence of a read
   * event: EVFILT_READ with EV_EOF still carries the final bytes, so the read
   * callback must run and let its read() distinguish 0, >0, and -1.
   */
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

}  // namespace coropact::kqueue::detail
