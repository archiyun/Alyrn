// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <sys/epoll.h>

#include <vector>

#include "vexo/net/poller.h"

namespace vexo::net {

// EPollPoller is the Linux epoll-based implementation of Poller.
//
// It translates Channel registrations into epoll_ctl operations and converts
// epoll_wait results back into active Channel objects for the owning EventLoop.
class EPollPoller final : public Poller {
public:
  explicit EPollPoller(EventLoop* loop);
  ~EPollPoller() override;

  [[nodiscard]] vexo::time::Timestamp Poll(int timeout_ms,
                                ChannelList* active_channels) override;
  void UpdateChannel(Channel* channel) override;
  void RemoveChannel(Channel* channel) override;

private:
  // Applies an add, modify, or delete operation to the underlying epoll fd.
  void Update(int operation, Channel* channel);

  // Converts ready epoll events into active Channel objects.
  void FillActiveChannels(int num_events,
                          ChannelList* active_channels) const;

  // Initial capacity of the epoll event buffer. The buffer may grow when the
  // number of returned events reaches the current capacity.
  static constexpr int kInitEventListSize = 16;

  int epollfd_;
  std::vector<epoll_event> events_;
};

}  // namespace vexo::net
