// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/reactor/poller.h"

#include "coropact/reactor/channel.h"
#include "coropact/reactor/epoll_poller.h"

namespace coropact::reactor {

Poller::Poller(EventLoop* loop)
    : owner_loop_(loop) {}

bool Poller::HasChannel(Channel* channel) const {
  auto it = channels_.find(channel->fd());
  return it != channels_.end() && it->second == channel;
}

Poller* Poller::NewDefaultPoller(EventLoop* loop) {
  return new EPollPoller(loop);
}

}  // namespace coropact::reactor
