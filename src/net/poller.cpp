#include "runtime/net/poller.h"
#include "runtime/net/channel.h"
#include "runtime/net/epoll_poller.h"

namespace runtime::net {

Poller::Poller(EventLoop* loop)
    : ownerLoop_(loop) {}

bool Poller::HasChannel(Channel* channel) const {
  auto it = channels_.find(channel->Fd());
  return it != channels_.end() && it->second == channel;
}

Poller* Poller::NewDefaultPoller(EventLoop* loop) {
  // The current networking layer uses epoll as the default I/O multiplexer.
  return new EPollPoller(loop);
}

}  // namespace runtime::net
