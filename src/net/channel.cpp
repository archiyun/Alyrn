#include "runtime/net/channel.h"
#include "runtime/net/event_loop.h"

#include <sys/epoll.h>

namespace runtime::net {

// A Channel starts with no interested events.
// Read events include normal readable data and priority data.
// Write events indicate that the fd can accept more output.
const int Channel::kNoneEvent = 0;
const int Channel::kReadEvent = EPOLLIN | EPOLLPRI;
const int Channel::kWriteEvent = EPOLLOUT;

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop),
      fd_(fd),
      events_(0),
      revents_(0),
      index_(-1),
      tied_(false) {}

Channel::~Channel() = default;

void Channel::Tie(const std::shared_ptr<void>& obj) {
  tie_ = obj;
  tied_ = true;
}

void Channel::Update() {
  loop_->UpdateChannel(this);
}

void Channel::Remove() {
  loop_->RemoveChannel(this);
}

void Channel::HandleEvent(runtime::time::Timestamp receive_time) {
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
  // EPOLLHUP without EPOLLIN usually means the peer has closed the connection
  // and there is no more readable data left in the socket buffer.
  if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
    if (close_callback_) {
      close_callback_();
    }
  }

  if (revents_ & EPOLLERR) {
    if (error_callback_) {
      error_callback_();
    }
  }

  if (revents_ & (EPOLLIN | EPOLLPRI)) {
    if (read_callback_) {
      read_callback_(receive_time);
    }
  }

  if (revents_ & EPOLLOUT) {
    if (write_callback_) {
      write_callback_();
    }
  }
}

}  // namespace runtime::net
