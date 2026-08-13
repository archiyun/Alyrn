// SPDX-License-Identifier: MIT
#pragma once

#include <sys/event.h>

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "coropact/kqueue/options.h"
#include "coropact/utils/macros.h"

namespace coropact::kqueue::detail {

class Channel;

/*
 * Readiness multiplexing for one KqueueLoop.
 *
 * A kqueue registration is keyed by (ident, filter), so read and write
 * interest are two independent kevents for the same descriptor. This poller
 * therefore stores what the kernel currently holds per filter and submits only
 * the difference when a Channel's interest set changes. It intentionally has
 * no abstract base: kqueue has exactly one implementation, and a second one
 * would be a separate backend rather than a second poller.
 *
 * Every method is owner-thread-only. The poller holds no back pointer to its
 * loop because the loop already rejects foreign-thread calls before
 * delegating here.
 */
class KqueuePoller final {
public:
  COROPACT_DELETE_COPY_MOVE(KqueuePoller);

  using ChannelList = std::vector<Channel*>;

  KqueuePoller();
  ~KqueuePoller();

  // Waits for readiness and fills active_channels with the Channels that
  // became active. A Channel appears at most once even when both of its
  // filters fire.
  void Poll(int timeout_ms, ChannelList* active_channels);

  // Reconciles the kernel registration with the Channel's current interest set.
  void UpdateChannel(Channel* channel);

  // Drops every filter registered for the Channel and forgets it.
  void RemoveChannel(Channel* channel);

  [[nodiscard]]
  bool HasChannel(Channel* channel) const;

private:
  /*
   * What the kernel currently holds for one descriptor, not what the Channel
   * wants. UpdateChannel submits the difference between the two.
   *
   * Under TriggerMode::kOneShot the kernel drops a filter as it delivers it,
   * so this mirror is also updated from the wait path, not only from
   * UpdateChannel.
   */
  struct Registration {
    Channel* channel{nullptr};
    bool read_enabled{false};
    bool write_enabled{false};
    TriggerMode mode{TriggerMode::kLevelTriggered};
  };

  // Submits one change and fails fast if the kernel rejects it. Changes are
  // applied immediately rather than batched into the Poll() call: a batched
  // change can outlive the descriptor it names and come back as EBADF.
  void ApplyChange(Channel* channel, std::int16_t filter, std::uint16_t flags);

  // Withdraws a one-shot filter that the kernel removed as it delivered it,
  // keeping both this mirror and the Channel's interest set truthful.
  void RetireOneShot(Registration& registration, Channel* channel, std::int16_t filter);

  void FillActiveChannels(int num_events, ChannelList* active_channels);

  static constexpr int kInitEventListSize = 16;

  int kqfd_;
  std::vector<struct kevent> events_;
  std::unordered_map<int, Registration> registrations_;

  // Distinguishes the first kevent for a Channel within one Poll() from later
  // ones for the same Channel.
  std::uint64_t poll_epoch_{0};
};

}  // namespace coropact::kqueue::detail
