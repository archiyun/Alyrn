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

enum class ChannelState : int {
  kNew = -1,
  kAdded = 1,
  kDeleted = 2,
};

const char *OpName(int op) {
  switch (op) {
  case EPOLL_CTL_ADD: return "ADD";
  case EPOLL_CTL_MOD: return "MOD";
  case EPOLL_CTL_DEL: return "DEL";
  default:            return "UNKNOWN";
  }
}

} // namespace

EPollPoller::EPollPoller(EventLoop *loop)
    : Poller(loop), epollfd_(::epoll_create1(EPOLL_CLOEXEC)),
      events_(kInitEventListSize) {
  if (epollfd_ < 0) {
    LOG_FATAL() << "epoll_create1 failed: errno=" << errno
                << " message=" << std::strerror(errno);
    std::abort();
  }
}

EPollPoller::~EPollPoller() {
  if (epollfd_ >= 0)
    ::close(epollfd_);
}

runtime::time::Timestamp EPollPoller::Poll(int timeout_ms,
                                           ChannelList *active_channels) {
  const int max_events = static_cast<int>(events_.size());
  int num_events =
      ::epoll_wait(epollfd_, events_.data(), events_.size(), timeout_ms);
  const int saved_errno = errno;
  auto now = runtime::time::Timestamp::Now();

  if (num_events > 0) {
    FillActiveChannels(num_events, active_channels);
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

void EPollPoller::FillActiveChannels(int num_events,
                                     ChannelList *active_channels) const {
  active_channels->reserve(active_channels->size() +
                           static_cast<size_t>(num_events));
  for (int i = 0; i < num_events; ++i) {
    auto *channel = static_cast<Channel *>(events_[i].data.ptr);
    channel->SetRevents(events_[i].events);
    active_channels->push_back(channel);
  }
}

void EPollPoller::UpdateChannel(Channel *channel) {
  const int fd = channel->Fd();
  const auto state = static_cast<ChannelState>(channel->Index());

  if (state == ChannelState::kNew || state == ChannelState::kDeleted) {
    if (state == ChannelState::kNew) {
      channels_[fd] = channel;
    }
    channel->SetIndex(static_cast<int>(ChannelState::kAdded));
    Update(EPOLL_CTL_ADD, channel);
    return;
  }

  // state == kAdded
  if (channel->IsNoneEvent()) {
    Update(EPOLL_CTL_DEL, channel);
    channel->SetIndex(static_cast<int>(ChannelState::kDeleted));
  } else {
    Update(EPOLL_CTL_MOD, channel);
  }
}

void EPollPoller::RemoveChannel(Channel *channel) {
  const int fd = channel->Fd();
  channels_.erase(fd);

  if (static_cast<ChannelState>(channel->Index()) == ChannelState::kAdded) {
    Update(EPOLL_CTL_DEL, channel);
  }

  channel->SetIndex(static_cast<int>(ChannelState::kNew));
}

void EPollPoller::Update(int operation, Channel *channel) {
  struct epoll_event event{};
  event.events = channel->Events();
  event.data.ptr = channel;

  if (::epoll_ctl(epollfd_, operation, channel->Fd(), &event) < 0) {
    LOG_ERROR() << "epoll_ctl failed: op=" << OpName(operation)
                << " fd=" << channel->Fd()
                << " events=" << channel->Events()
                << " errno=" << errno
                << " message=" << std::strerror(errno);
  }
}
} // namespace runtime::net
