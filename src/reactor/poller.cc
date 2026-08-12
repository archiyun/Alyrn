// SPDX-License-Identifier: MIT
#include "coropact/reactor/detail/poller.h"

#include "coropact/reactor/detail/channel.h"
#include "coropact/reactor/detail/epoll_poller.h"

namespace coropact::reactor::detail {

bool Poller::HasChannel(Channel* channel) const {
  auto it = channels_.find(channel->Fd());
  return it != channels_.end() && it->second == channel;
}

Poller* Poller::NewDefaultPoller() { return new EPollPoller(); }

}  // namespace coropact::reactor::detail
