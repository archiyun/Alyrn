// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/net/poller.h"

#include "coropact/net/channel.h"
#include "coropact/net/epoll_poller.h"

namespace coropact::net {

Poller::Poller(EventLoop* loop)
    : owner_loop_(loop) {}

bool Poller::HasChannel(Channel* channel) const {
  auto it = channels_.find(channel->fd());
  return it != channels_.end() && it->second == channel;
}

Poller* Poller::NewDefaultPoller(EventLoop* loop) {
  return new EPollPoller(loop);
}

}  // namespace coropact::net
