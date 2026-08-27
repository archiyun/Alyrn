// SPDX-License-Identifier: MIT
#include "alyrn/reactor/detail/poller.h"

#include "alyrn/reactor/detail/channel.h"
#include "alyrn/reactor/detail/epoll_poller.h"

namespace alyrn::reactor::detail {

bool Poller::HasChannel(Channel* channel) const {
  auto it = channels_.find(channel->Fd());
  return it != channels_.end() && it->second == channel;
}

Poller* Poller::NewDefaultPoller() { return new EPollPoller(); }

}  // namespace alyrn::reactor::detail
