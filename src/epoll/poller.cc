// SPDX-License-Identifier: MIT
#include "alyrn/epoll/detail/poller.h"

#include "alyrn/epoll/detail/channel.h"
#include "alyrn/epoll/detail/epoll_poller.h"

namespace alyrn::epoll::detail {

bool Poller::HasChannel(Channel* channel) const {
  auto it = channels_.find(channel->Fd());
  return it != channels_.end() && it->second == channel;
}

std::unique_ptr<Poller> Poller::NewDefaultPoller() { return std::make_unique<EPollPoller>(); }

}  // namespace alyrn::epoll::detail
