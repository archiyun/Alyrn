// SPDX-License-Identifier: MIT
#include "alyrn/epoll/detail/epoll_poller.h"

#include <unistd.h>

#include <cerrno>

#include "alyrn/detail/check.h"
#include "alyrn/epoll/detail/channel.h"

namespace alyrn::epoll::detail {
namespace {

// Channel registration state within EPollPoller.
enum class ChannelState : int8_t {
  kNew = -1,
  kAdded = 1,
  kDeleted = 2,
};

// Entrance， Channel->events()
static uint32_t ToEpollEvents(int abstract_events) {
  uint32_t event{0};
  if (static_cast<bool>(abstract_events & Channel::kReadEvent)) {
    event |= EPOLLIN | EPOLLPRI | EPOLLRDHUP;
  }
  if (static_cast<bool>(abstract_events & Channel::kWriteEvent)) {
    event |= EPOLLOUT;
  }
  return event;
}

// Result, epoll_event.events -> abstract
static int FromEpollEvents(uint32_t epoll_events) {
  int event{0};
  if (static_cast<bool>(epoll_events & (EPOLLIN | EPOLLPRI))) {
    event |= Channel::kReadEvent;
  }
  if (static_cast<bool>(epoll_events & EPOLLOUT)) {
    event |= Channel::kWriteEvent;
  }
  if (static_cast<bool>(epoll_events & EPOLLERR)) {
    event |= Channel::kErrorEvent;
  }
  if (static_cast<bool>(epoll_events & (EPOLLHUP | EPOLLRDHUP))) {
    event |= Channel::kHupEvent;
  }
  return event;
}

}  // namespace

EPollPoller::EPollPoller()
    : epollfd_(::epoll_create1(EPOLL_CLOEXEC)), events_(kInitEventListSize) {
  if (epollfd_ < 0) {
    ALYRN_CHECK(false, "EPollPoller: epoll_create1 failed");
  }
}

EPollPoller::~EPollPoller() {
  if (epollfd_ >= 0) {
    ::close(epollfd_);
  }
}

void EPollPoller::Poll(int timeout_ms, ChannelList* active_channels) {
  const int max_events = static_cast<int>(events_.size());
  const int num_events = ::epoll_wait(epollfd_, events_.data(), max_events, timeout_ms);
  const int saved_errno = errno;

  if (num_events > 0) {
    FillActiveChannels(num_events, active_channels);

    // Grow the event buffer when epoll_wait fills the current capacity so the
    // next poll can absorb a larger burst of ready fds.
    if (num_events == max_events) {
      events_.resize(events_.size() * 2);
    }
  } else if (num_events < 0 && saved_errno != EINTR) {
    ALYRN_CHECK(false, "EPollPoller: epoll_wait failed");
  }

}

void EPollPoller::FillActiveChannels(int num_events, ChannelList* active_channels) const {
  active_channels->reserve(active_channels->size() + static_cast<std::size_t>(num_events));
  for (int i = 0; i < num_events; ++i) {
    auto* channel = static_cast<Channel*>(events_[i].data.ptr);
    channel->SetRevents(FromEpollEvents(events_[i].events));
    active_channels->push_back(channel);
  }
}

void EPollPoller::UpdateChannel(Channel* channel) {
  const int fd = channel->Fd();
  const auto state = static_cast<ChannelState>(channel->index());

  // New and previously deleted channels both need an ADD before they can
  // receive events from epoll again.
  if (state == ChannelState::kNew || state == ChannelState::kDeleted) {
    if (state == ChannelState::kNew) {
      channels_[fd] = channel;
    }
    channel->set_index(static_cast<int>(ChannelState::kAdded));
    Update(EPOLL_CTL_ADD, channel);
    return;
  }

  // Channels already registered in epoll transition between MOD and DEL
  // depending on whether they still care about any events.
  if (channel->IsNoneEvent()) {
    Update(EPOLL_CTL_DEL, channel);
    channel->set_index(static_cast<int>(ChannelState::kDeleted));
  } else {
    Update(EPOLL_CTL_MOD, channel);
  }
}

void EPollPoller::RemoveChannel(Channel* channel) {
  const int fd = channel->Fd();
  channels_.erase(fd);

  if (static_cast<ChannelState>(channel->index()) == ChannelState::kAdded) {
    Update(EPOLL_CTL_DEL, channel);
  }

  channel->set_index(static_cast<int>(ChannelState::kNew));
}

void EPollPoller::Update(int operation, Channel* channel) {
  epoll_event event{};
  event.events = ToEpollEvents(channel->Events());
  if (channel->IsEdgeTriggered()) {
    // Preserve the caller's edge-triggered preference in epoll.
    event.events |= EPOLLET;
  }
  event.data.ptr = channel;

  if (::epoll_ctl(epollfd_, operation, channel->Fd(), &event) < 0) {
    ALYRN_CHECK(false, "EPollPoller: epoll_ctl failed");
  }
}

}  // namespace alyrn::epoll::detail
