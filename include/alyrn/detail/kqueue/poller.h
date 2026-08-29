// SPDX-License-Identifier: MIT
#pragma once

#include <sys/event.h>

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "alyrn/kqueue/options.h"
#include "alyrn/detail/macros.h"

namespace alyrn::kqueue::detail {

class Channel;

/*
 * Readiness multiplexing for one Loop.
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
class Poller final {
public:
  ALYRN_DELETE_COPY_MOVE(Poller);

  using ChannelList = std::vector<Channel*>;

  Poller();
  ~Poller();

  // Waits for readiness and fills active_channels with the Channels that
  // became active. A Channel appears at most once even when both of its
  // filters fire.
  void Poll(int timeout_ms, ChannelList* active_channels);

  // Reconciles the kernel registration with the Channel's current interest set.
  void UpdateChannel(Channel* channel);

  // Drops every filter registered for the Channel and forgets it.
  void RemoveChannel(Channel* channel);

  bool HasChannel(Channel* channel) const;

  using TimerExpireHandler = void (*)(void* context) noexcept;

  // EVFILT_TIMER is not an fd Channel. The loop installs one expire handler
  // for the user-space timer queue; the ident is a dedicated non-fd key.
  void SetTimerExpireHandler(TimerExpireHandler handler, void* context) noexcept;

  // Arms a one-shot kernel alarm for the earliest user-space deadline.
  // EV_ADD on the same ident replaces an already-armed alarm.
  void ArmOneShotTimer(std::int64_t nsec);

  // Withdraws the kernel alarm if it is still pending. A no-op after oneshot
  // delivery, so EV_DELETE is never issued against a retired registration.
  void DisarmTimer();

  // Runs the expire handler after Channel::HandleEvent() for this poll, so a
  // descriptor that became ready in the same kevent batch is delivered before
  // a colliding SleepFor / RunAfter deadline.
  void DispatchTimerExpire();

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

  static constexpr std::uintptr_t kTimerIdent = 1;
  static constexpr std::int64_t kMinTimerNsec = 100000;  // 100 µs, matching timerfd

  void ApplyTimerChange(std::uint16_t flags, std::uint32_t fflags, intptr_t data);

  int kqfd_;
  std::vector<struct kevent> events_;
  std::unordered_map<int, Registration> registrations_;

  // Distinguishes the first kevent for a Channel within one Poll() from later
  // ones for the same Channel.
  std::uint64_t poll_epoch_{0};

  TimerExpireHandler timer_expire_handler_{nullptr};
  void* timer_expire_context_{nullptr};
  bool timer_armed_{false};
  bool timer_expired_this_poll_{false};
};

}  // namespace alyrn::kqueue::detail
