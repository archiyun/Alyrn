// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/reactor/detail/poller.h"

#include "coropact/reactor/detail/channel.h"
#include "coropact/reactor/detail/epoll_poller.h"

namespace coropact::reactor::detail {

Poller::Poller(EventLoop* loop)
    : owner_loop_(loop) {}

bool Poller::HasChannel(Channel* channel) const {
  auto it = channels_.find(channel->Fd());
  return it != channels_.end() && it->second == channel;
}

Poller* Poller::NewDefaultPoller(EventLoop* loop) {
  return new EPollPoller(loop);
}

}  // namespace coropact::reactor::detail
