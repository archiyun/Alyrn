// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/poller.h"

#include <cstdlib>
#include <string_view>

#include "vexo/net/channel.h"
#include "vexo/net/epoll_poller.h"
#include "vexo/net/poll_poller.h"
#include "vexo/net/select.h"

namespace vexo::net {

Poller::Poller(EventLoop* loop)
    : owner_loop_(loop) {}

bool Poller::HasChannel(Channel* channel) const {
  auto it = channels_.find(channel->fd());
  return it != channels_.end() && it->second == channel;
}

static Poller* DefaultPoller(EventLoop* loop) {
#ifdef __linux__
  return new EPollPoller(loop);
#else
  return new PollPoller(loop);
#endif
}

Poller* Poller::NewDefaultPoller(EventLoop* loop) {
  const char* env = ::getenv("VEXO_POLLER");
  if (env != nullptr) {
    const std::string_view name(env);
    if (name == "poll")   return new PollPoller(loop);
    if (name == "select") return new SelectPoller(loop);
#ifdef __linux__
    if (name == "epoll")  return new EPollPoller(loop);
#endif
  }
  return DefaultPoller(loop);
}

}  // namespace vexo::net
