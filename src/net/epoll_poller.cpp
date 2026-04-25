#include "runtime/net/epoll_poller.h"
#include "runtime/net/channel.h"
#include "runtime/log/logger.h"

#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

namespace runtime::net {
namespace {

// Channel registration state within EPollPoller.
enum class ChannelState : int {
  kNew = -1,
  kAdded = 1,
  kDeleted = 2,
};

const char* OpName(int op) {
  switch (op) {
    case EPOLL_CTL_ADD:
      return "ADD";
    case EPOLL_CTL_MOD:
      return "MOD";
    case EPOLL_CTL_DEL:
      return "DEL";
    default:
      return "UNKNOWN";
  }
}

// 入口方向， Channel->Events()
static uint32_t ToEpollEvents(int abstract_events) {
  uint32_t ev{0};
  if (abstract_events & Channel::kReadEvent)  ev |= EPOLLIN | EPOLLPRI;
  if (abstract_events & Channel::kWriteEvent) ev |= EPOLLOUT;
  return ev;
}

// 结果方向, epoll_event.events -> abstract
static int FromEpollEvents(uint32_t epoll_events) {
  int ev{0};
  if (epoll_events & (EPOLLIN | EPOLLPRI)) ev |= Channel::kReadEvent;
  if (epoll_events & EPOLLOUT)             ev |= Channel::kWriteEvent;
  if (epoll_events & EPOLLERR)             ev |= Channel::kErrorEvent;
  if (epoll_events & EPOLLHUP)             ev |= Channel::kHupEvent;
  return ev;
}

}  // namespace


EPollPoller::EPollPoller(EventLoop* loop)
    : Poller(loop),
      epollfd_(::epoll_create1(EPOLL_CLOEXEC)),
      events_(kInitEventListSize) {
  if (epollfd_ < 0) {
    LOG_FATAL() << "epoll_create1 failed: errno=" << errno
                << " message=" << std::strerror(errno);
    std::abort();
  }
}

EPollPoller::~EPollPoller() {
  if (epollfd_ >= 0) {
    ::close(epollfd_);
  }
}

runtime::time::Timestamp EPollPoller::Poll(
    int timeout_ms,
    ChannelList* active_channels) {
  const int max_events = static_cast<int>(events_.size());
  const int num_events =
      ::epoll_wait(epollfd_, events_.data(), max_events, timeout_ms);
  const int saved_errno = errno;
  const auto now = runtime::time::Timestamp::Now();

  if (num_events > 0) {
    FillActiveChannels(num_events, active_channels);

    // Grow the event buffer when epoll_wait fills the current capacity so the
    // next poll can absorb a larger burst of ready fds.
    if (num_events == max_events) {
      events_.resize(events_.size() * 2);
    }
  } else if (num_events < 0 && saved_errno != EINTR) {
    errno = saved_errno;
    LOG_ERROR() << "epoll_wait failed: errno=" << saved_errno
                << " message=" << std::strerror(saved_errno);
  }

  return now;
}

void EPollPoller::FillActiveChannels(
    int num_events,
    ChannelList* active_channels) const {
  active_channels->reserve(active_channels->size() +
                           static_cast<std::size_t>(num_events));
  for (int i = 0; i < num_events; ++i) {
    auto* channel = static_cast<Channel*>(events_[i].data.ptr);
    channel->SetRevents(FromEpollEvents(events_[i].events));
    active_channels->push_back(channel);
  }
}

void EPollPoller::UpdateChannel(Channel* channel) {
  const int fd = channel->Fd();
  const auto state = static_cast<ChannelState>(channel->Index());

  // New and previously deleted channels both need an ADD before they can
  // receive events from epoll again.
  if (state == ChannelState::kNew || state == ChannelState::kDeleted) {
    if (state == ChannelState::kNew) {
      channels_[fd] = channel;
    }
    channel->SetIndex(static_cast<int>(ChannelState::kAdded));
    Update(EPOLL_CTL_ADD, channel);
    return;
  }

  // Channels already registered in epoll transition between MOD and DEL
  // depending on whether they still care about any events.
  if (channel->IsNoneEvent()) {
    Update(EPOLL_CTL_DEL, channel);
    channel->SetIndex(static_cast<int>(ChannelState::kDeleted));
  } else {
    Update(EPOLL_CTL_MOD, channel);
  }
}

void EPollPoller::RemoveChannel(Channel* channel) {
  const int fd = channel->Fd();
  channels_.erase(fd);

  if (static_cast<ChannelState>(channel->Index()) == ChannelState::kAdded) {
    Update(EPOLL_CTL_DEL, channel);
  }

  channel->SetIndex(static_cast<int>(ChannelState::kNew));
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
    LOG_ERROR() << "epoll_ctl failed: op=" << OpName(operation)
                << " fd=" << channel->Fd()
                << " events=" << channel->Events()
                << " errno=" << errno
                << " message=" << std::strerror(errno);
  }
}

}  // namespace runtime::net
