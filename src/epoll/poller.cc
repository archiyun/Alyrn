// SPDX-License-Identifier: MIT
#include "alyrn/detail/epoll/poller.h"

#include "alyrn/detail/epoll/channel.h"
#include "alyrn/detail/epoll/epoll_poller.h"

namespace alyrn::epoll::detail {

bool Poller::HasChannel(Channel* channel) const {
  auto it = channels_.find(channel->Fd());
  return it != channels_.end() && it->second == channel;
}

Poller* Poller::NewDefaultPoller() { return new EPollPoller(); }

}  // namespace alyrn::epoll::detail
