#include "runtime/net/poller.h"
#include "runtime/net/channel.h"
#include "runtime/net/epoll_poller.h"
#include "runtime/net/poll_poller.h"
#include "runtime/net/select.h"

#include <cstdlib>
#include <string_view>

namespace runtime::net {

Poller::Poller(EventLoop* loop)
    : ownerLoop_(loop) {}

bool Poller::HasChannel(Channel* channel) const {
  auto it = channels_.find(channel->Fd());
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
  const char* env = ::getenv("RUNTIME_POLLER");
  if (env != nullptr) {
    const std::string_view name(env);
    if (name == "poll")   return new PollPoller(loop);
    if (name == "select") return new SelectPoller(loop);
#ifdef __linux__
    if (name == "epoll")  return new EPollPoller(loop);
#endif
    // 未知名称：忽略，使用平台默认
  }
  return DefaultPoller(loop);
}

}  // namespace runtime::net
