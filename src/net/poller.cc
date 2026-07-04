// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/poller.h"

#include "vexo/net/channel.h"
#include "vexo/net/epoll_poller.h"

namespace vexo::net {

Poller::Poller(EventLoop* loop)
    : owner_loop_(loop) {}

bool Poller::HasChannel(Channel* channel) const {
  auto it = channels_.find(channel->fd());
  return it != channels_.end() && it->second == channel;
}

Poller* Poller::NewDefaultPoller(EventLoop* loop) {
  return new EPollPoller(loop);
}

}  // namespace vexo::net
