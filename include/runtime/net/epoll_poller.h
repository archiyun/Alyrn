#pragma once

#include "runtime/net/poller.h"

#include <sys/epoll.h>
#include <vector>

namespace runtime::net {

// EPollPoller is the Linux epoll-based implementation of Poller.
//
// It translates Channel registrations into epoll_ctl operations and converts
// epoll_wait results back into active Channel objects for the owning EventLoop.
class EPollPoller : public Poller {
public:
  explicit EPollPoller(EventLoop* loop);
  ~EPollPoller() override;

  runtime::time::Timestamp Poll(int timeout_ms,
                                ChannelList* active_channels) override;
  void UpdateChannel(Channel* channel) override;
  void RemoveChannel(Channel* channel) override;

private:
  // Applies an add, modify, or delete operation to the underlying epoll fd.
  void Update(int operation, Channel* channel);

  // Converts ready epoll events into active Channel objects.
  void FillActiveChannels(int num_events,
                          ChannelList* active_channels) const;

private:
  // Initial capacity of the epoll event buffer. The buffer may grow when the
  // number of returned events reaches the current capacity.
  static constexpr int kInitEventListSize = 16;

  int epollfd_;
  std::vector<epoll_event> events_;
};

}  // namespace runtime::net
