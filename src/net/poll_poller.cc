// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/poll_poller.h"

#include <cassert>
#include <cerrno>
#include <cstring>

#include "vexo/log/logger.h"
#include "vexo/net/channel.h"

namespace vexo::net {

namespace {

short ToPollEvents(int abstract_events) {
    short ev{0};
    if (abstract_events & Channel::kReadEvent)  ev |= POLLIN | POLLPRI;
    if (abstract_events & Channel::kWriteEvent) ev |= POLLOUT;
    return ev;
}

int FromPollEvents(short poll_events) {
    int ev{0};
    if (poll_events &(POLLIN | POLLPRI))   ev |= Channel::kReadEvent;
    if (poll_events & POLLOUT)             ev |= Channel::kWriteEvent;
    if (poll_events & POLLERR)             ev |= Channel::kErrorEvent;
    if (poll_events & POLLHUP)             ev |= Channel::kHupEvent;
    return ev;
}

}  // namespace


PollPoller::PollPoller(EventLoop* loop) : Poller(loop) {}

vexo::time::Timestamp PollPoller::Poll(int timeout_ms,
                                         ChannelList* active_channels) {
  const int num_events = ::poll(pollfds_.data(), static_cast<nfds_t>(pollfds_.size()), timeout_ms);
  const int saved_errno = errno;
  const auto now = vexo::time::Timestamp::Now();

  if (num_events > 0) {
    FillActiveChannels(num_events, active_channels);
  } else if(num_events < 0 && saved_errno != EINTR) {
    errno = saved_errno;
    LOG_ERROR() << "poll failed: errno=" << saved_errno
                << " message=" << std::strerror(saved_errno);
  }
  return now;
}

void PollPoller::FillActiveChannels(int num_events, ChannelList* active_channels) const {
  for(const auto& pfd : pollfds_) {
    if (pfd.revents == 0) continue;
    if (num_events-- == 0) break;
    auto it = channels_.find(pfd.fd);
    assert(it != channels_.end());
    Channel* channel = it->second;
    channel->set_revents(FromPollEvents(pfd.revents));
    active_channels->push_back(channel);
  }
}
void PollPoller::UpdateChannel(Channel* channel) {
  const int idx = channel->index();
  if (idx < 0) {
    assert(channels_.find(channel->fd()) == channels_.end());
    channels_[channel->fd()] = channel;
    pollfd pfd{};
    pfd.fd = channel->fd();
    pfd.events = ToPollEvents(channel->events());
    pollfds_.push_back(pfd);
    channel->set_index(static_cast<int>(pollfds_.size() - 1));
    return;
  }
  assert(channels_.find(channel->fd()) != channels_.end());
  auto& pfd = pollfds_[idx];
  pfd.events = ToPollEvents(channel->events());
  pfd.revents = 0;
  // 全部事件禁用时置负 fd，poll() 忽略；重新 enable 时恢复正 fd
  pfd.fd = channel->IsNoneEvent() ? (-channel->fd() - 1) : channel->fd();
}

void PollPoller::RemoveChannel(Channel* channel) {
  assert(channel->IsNoneEvent());
  const int idx = channel->index();
  const int fd = channel->fd();
  assert(channels_.find(fd) != channels_.end());
  channels_.erase(fd);

  const int last_idx = static_cast<int>(pollfds_.size() - 1);
  if (idx != last_idx) {
    const auto& last_pfd = pollfds_[last_idx];
    const int last_fd = last_pfd.fd >= 0 ? last_pfd.fd : (-last_pfd.fd - 1);
    channels_[last_fd]->set_index(idx);
    pollfds_[idx] = last_pfd;
   }
  pollfds_.pop_back();
  channel->set_index(-1);
}

} // namespace vexo::net