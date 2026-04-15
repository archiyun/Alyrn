#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/time/timestamp.h"

#include <unordered_map>
#include <vector>

namespace runtime::net {

class Channel;
class EventLoop;

// Poller is the user-space abstraction over the underlying I/O multiplexing
// mechanism, such as epoll.
//
// Each EventLoop owns exactly one Poller. The Poller tracks the Channels
// currently registered with the loop, waits for active events from the kernel,
// and updates or removes Channel registrations as their interest sets change.
class Poller : public runtime::base::NonCopyable {
public:
  using ChannelList = std::vector<Channel*>;

  explicit Poller(EventLoop* loop);
  virtual ~Poller() = default;

  // Waits for I/O events and fills active_channels with the Channels that
  // became active before the timeout expires.
  virtual runtime::time::Timestamp Poll(int timeout_ms,
                                        ChannelList* active_channels) = 0;

  // Adds or updates a Channel registration in the underlying poller.
  virtual void UpdateChannel(Channel* channel) = 0;

  // Removes a Channel registration from the underlying poller.
  virtual void RemoveChannel(Channel* channel) = 0;

  // Returns true if channel is currently tracked by this Poller.
  bool HasChannel(Channel* channel) const;

  // Creates the default Poller implementation for the current platform.
  static Poller* NewDefaultPoller(EventLoop* loop);

protected:
  using ChannelMap = std::unordered_map<int, Channel*>;
  ChannelMap channels_;

private:
  // The owning EventLoop. Poller follows the one-loop-per-thread model.
  EventLoop* ownerLoop_;
};

}  // namespace runtime::net
