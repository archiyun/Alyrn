// SPDX-License-Identifier: MIT
#include "alyrn/detail/kqueue/poller.h"

#include <fcntl.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <limits>

#include "alyrn/detail/check.h"
#include "alyrn/detail/kqueue/channel.h"

namespace alyrn::kqueue::detail {
namespace {

/*
 * Maps one returned kevent to the logical Channel event bits.
 *
 * EV_EOF on EVFILT_READ still carries the final bytes, so kReadEvent is
 * reported alongside kHupEvent and the read callback decides what the peer
 * state means. The socket error, when there is one, arrives in fflags rather
 * than as a distinct event; EV_ERROR is not part of this mapping because it
 * reports a rejected changelist entry, not a descriptor condition.
 */
int FromKevent(const struct kevent& event) {
  int revents = Channel::kNoneEvent;

  if (event.filter == EVFILT_READ) {
    revents |= Channel::kReadEvent;
  } else if (event.filter == EVFILT_WRITE) {
    revents |= Channel::kWriteEvent;
  }

  if (static_cast<bool>(event.flags & EV_EOF)) {
    revents |= Channel::kHupEvent;
    if (event.fflags != 0) {
      revents |= Channel::kErrorEvent;
    }
  }

  return revents;
}

/*
 * Translates a trigger mode into the flags that arm one filter.
 *
 * EV_ADD on an existing (ident, filter) pair replaces its flags in place, so
 * the same value both arms a new filter and switches the mode of an armed
 * one. EV_CLEAR and EV_ONESHOT are mutually exclusive here by construction:
 * a registration the kernel discards on delivery has no edge left to report.
 */
std::uint16_t ArmFlagsFor(TriggerMode mode) {
  switch (mode) {
    case TriggerMode::kEdgeTriggered:
      return static_cast<std::uint16_t>(EV_ADD | EV_ENABLE | EV_CLEAR);
    case TriggerMode::kOneShot:
      return static_cast<std::uint16_t>(EV_ADD | EV_ENABLE | EV_ONESHOT);
    case TriggerMode::kLevelTriggered:
      break;
  }
  return static_cast<std::uint16_t>(EV_ADD | EV_ENABLE);
}

}  // namespace

Poller::Poller() : kqfd_(::kqueue()), events_(kInitEventListSize) {
  ALYRN_CHECK(kqfd_ >= 0, "Poller: kqueue creation failed");
  /* kqueue descriptors are not inherited across fork, but exec still needs
   * an explicit close-on-exec because kqueue() takes no flags argument. */
  ALYRN_CHECK(::fcntl(kqfd_, F_SETFD, FD_CLOEXEC) == 0,
                 "Poller: failed to set FD_CLOEXEC");
}

Poller::~Poller() {
  if (kqfd_ >= 0) {
    ::close(kqfd_);
  }
}

void Poller::Poll(int timeout_ms, ChannelList* active_channels) {
  timer_expired_this_poll_ = false;

  struct timespec timeout{};
  timeout.tv_sec = timeout_ms / 1000;
  timeout.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1000000L;

  const int max_events = static_cast<int>(events_.size());
  const int num_events = ::kevent(kqfd_, nullptr, 0, events_.data(), max_events, &timeout);
  const int saved_errno = errno;

  if (num_events > 0) {
    FillActiveChannels(num_events, active_channels);

    // Grow the event buffer when kevent fills the current capacity so the next
    // poll can absorb a larger burst of ready descriptors.
    if (num_events == max_events) {
      events_.resize(events_.size() * 2);
    }
  } else if (num_events < 0 && saved_errno != EINTR) {
    ALYRN_CHECK(false, "Poller: kevent wait failed");
  }
}

void Poller::FillActiveChannels(int num_events, ChannelList* active_channels) {
  ++poll_epoch_;
  active_channels->reserve(active_channels->size() + static_cast<std::size_t>(num_events));

  for (int i = 0; i < num_events; ++i) {
    const struct kevent& event = events_[i];

    /* No changelist is submitted with the wait above, so EV_ERROR here would
     * mean the kernel rejected a change this poller never sent. */
    ALYRN_CHECK(!static_cast<bool>(event.flags & EV_ERROR),
                   "Poller: kevent reported EV_ERROR during wait");

    /* EVFILT_TIMER is keyed by a dedicated ident, not a Channel fd. The
     * oneshot is already gone from the kernel; remember that so DisarmTimer
     * does not issue EV_DELETE, and so HandleEvent can still run first. */
    if (event.filter == EVFILT_TIMER) {
      timer_armed_ = false;
      timer_expired_this_poll_ = true;
      continue;
    }

    auto* channel = static_cast<Channel*>(event.udata);
    const int revents = FromKevent(event);

    /*
     * A one-shot filter is gone from the kernel the moment it is delivered.
     * Retiring it before HandleEvent() runs is what makes a callback that
     * re-arms observe a real interest change, and what stops UpdateChannel
     * from later issuing EV_DELETE for a registration that no longer exists.
     */
    const auto entry = registrations_.find(channel->Fd());
    if (entry != registrations_.end() && entry->second.mode == TriggerMode::kOneShot) {
      RetireOneShot(entry->second, channel, event.filter);
    }

    /*
     * Read and write readiness arrive as separate kevents for one descriptor,
     * so a Channel can appear twice in this loop. Overwriting on the second
     * sight would drop the first filter's bits, and pushing twice would run
     * HandleEvent() twice for a single readiness turn.
     */
    if (channel->ActiveEpoch() == poll_epoch_) {
      channel->SetRevents(channel->Revents() | revents);
      continue;
    }

    channel->SetActiveEpoch(poll_epoch_);
    channel->SetRevents(revents);
    active_channels->push_back(channel);
  }
}

/*
 * A previously unknown fd default-constructs an all-false Registration, which
 * is exactly the right starting point for the diff below: nothing is
 * registered yet, so every wanted filter needs an EV_ADD.
 */
void Poller::UpdateChannel(Channel* channel) {
  Registration& registration = registrations_[channel->Fd()];
  registration.channel = channel;

  const bool want_read = channel->IsReading();
  const bool want_write = channel->IsWriting();
  const TriggerMode want_mode = channel->Mode();

  /* A trigger-mode change cannot be expressed as an update, so an already
   * enabled filter is re-added under the new mode's flags. */
  const bool retrigger = registration.mode != want_mode;
  const std::uint16_t arm_flags = ArmFlagsFor(want_mode);

  /*
   * Only the difference is submitted. Under kOneShot the read_enabled and
   * write_enabled mirrors are cleared by RetireOneShot() as events are
   * delivered, so a Channel that re-arms from its callback takes the EV_ADD
   * branch here rather than being mistaken for already registered.
   */
  if (want_read && (!registration.read_enabled || retrigger)) {
    ApplyChange(channel, EVFILT_READ, arm_flags);
  } else if (!want_read && registration.read_enabled) {
    ApplyChange(channel, EVFILT_READ, EV_DELETE);
  }

  if (want_write && (!registration.write_enabled || retrigger)) {
    ApplyChange(channel, EVFILT_WRITE, arm_flags);
  } else if (!want_write && registration.write_enabled) {
    ApplyChange(channel, EVFILT_WRITE, EV_DELETE);
  }

  registration.read_enabled = want_read;
  registration.write_enabled = want_write;
  registration.mode = want_mode;
}

void Poller::RetireOneShot(Registration& registration, Channel* channel,
                                 std::int16_t filter) {
  if (filter == EVFILT_READ) {
    registration.read_enabled = false;
    channel->ConsumeOneShot(Channel::kReadEvent);
  } else if (filter == EVFILT_WRITE) {
    registration.write_enabled = false;
    channel->ConsumeOneShot(Channel::kWriteEvent);
  }
}

void Poller::RemoveChannel(Channel* channel) {
  const auto entry = registrations_.find(channel->Fd());
  if (entry == registrations_.end()) {
    return;
  }

  /* Channel::Remove() already required an empty interest set, so on the normal
   * path both filters are gone and only the bookkeeping erase runs. The
   * deletes below cover a Channel dropped without that step. */
  const Registration& registration = entry->second;
  if (registration.read_enabled) {
    ApplyChange(channel, EVFILT_READ, EV_DELETE);
  }
  if (registration.write_enabled) {
    ApplyChange(channel, EVFILT_WRITE, EV_DELETE);
  }

  registrations_.erase(entry);
}

bool Poller::HasChannel(Channel* channel) const {
  const auto entry = registrations_.find(channel->Fd());
  return entry != registrations_.end() && entry->second.channel == channel;
}

void Poller::ApplyChange(Channel* channel, std::int16_t filter, std::uint16_t flags) {
  struct kevent change{};
  EV_SET(&change, static_cast<std::uintptr_t>(channel->Fd()), filter, flags, 0, 0, channel);

  /* Submitting with an empty eventlist reports failure through the return
   * value instead of an EV_ERROR record, so a rejected change cannot be
   * mistaken for a descriptor condition later. */
  if (::kevent(kqfd_, &change, 1, nullptr, 0, nullptr) < 0) {
    ALYRN_CHECK(false, "Poller: kevent change failed");
  }
}

void Poller::SetTimerExpireHandler(TimerExpireHandler handler, void* context) noexcept {
  timer_expire_handler_ = handler;
  timer_expire_context_ = context;
}

void Poller::ArmOneShotTimer(std::int64_t nsec) {
  if (nsec < kMinTimerNsec) {
    nsec = kMinTimerNsec;
  }
  const auto max_nsec = static_cast<std::int64_t>(std::numeric_limits<intptr_t>::max());
  if (nsec > max_nsec) {
    nsec = max_nsec;
  }

  std::uint32_t fflags = 0;
  intptr_t data = static_cast<intptr_t>(nsec);
#if defined(NOTE_NSECONDS)
  fflags = NOTE_NSECONDS;
#elif defined(NOTE_USECONDS)
  fflags = NOTE_USECONDS;
  data = static_cast<intptr_t>(nsec / 1000);
  if (data < 1) {
    data = 1;
  }
#else
  data = static_cast<intptr_t>(nsec / 1000000);
  if (data < 1) {
    data = 1;
  }
#endif

  ApplyTimerChange(static_cast<std::uint16_t>(EV_ADD | EV_ENABLE | EV_ONESHOT), fflags, data);
  timer_armed_ = true;
}

void Poller::DisarmTimer() {
  if (!timer_armed_) {
    return;
  }
  ApplyTimerChange(EV_DELETE, 0, 0);
  timer_armed_ = false;
}

void Poller::DispatchTimerExpire() {
  if (!timer_expired_this_poll_) {
    return;
  }
  timer_expired_this_poll_ = false;
  if (timer_expire_handler_ != nullptr) {
    timer_expire_handler_(timer_expire_context_);
  }
}

void Poller::ApplyTimerChange(std::uint16_t flags, std::uint32_t fflags, intptr_t data) {
  struct kevent change{};
  EV_SET(&change, kTimerIdent, EVFILT_TIMER, flags, fflags, data, nullptr);

  if (::kevent(kqfd_, &change, 1, nullptr, 0, nullptr) < 0) {
    if (static_cast<bool>(flags & EV_DELETE) && errno == ENOENT) {
      return;
    }
    ALYRN_CHECK(false, "Poller: EVFILT_TIMER change failed");
  }
}

}  // namespace alyrn::kqueue::detail
