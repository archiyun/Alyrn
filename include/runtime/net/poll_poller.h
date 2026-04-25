#pragma once

#include "runtime/net/poller.h"

#include <sys/poll.h>
#include <vector>

namespace runtime::net {

// PollPoller is the poll(2)-based implementation of Poller.
// It is used on platforms that do not support epoll, such as macOS and
// BSD variants. It translates Channel registrations into pollfd structures and
// converts poll results back into active Channel
class PollPoller : public Poller {
 public:
  explicit PollPoller(EventLoop* loop);
  ~PollPoller() override = default;

  runtime::time::Timestamp Poll(int timeout_ms,
                                ChannelList* active_channels) override;
  void UpdateChannel(Channel* channel) override;
  void RemoveChannel(Channel* channel) override;

 private:
  void FillActiveChannels(int num_events,
                          ChannelList* active_channels) const;

  std::vector<pollfd> pollfds_;
};

}  // namespace runtime::net