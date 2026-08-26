// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "coropact/ds/mpsc_bounded_queue.h"

namespace coropact::luring::detail {

inline constexpr std::uint64_t kMsgRingNotificationUserData = 1;

struct Message {
  std::uint64_t data{0};
};

enum class MailboxPushResult : std::uint8_t {
  kQueuedNeedsNotification,
  kQueued,
  kFull,
};

class Mailbox {
public:
  static constexpr std::size_t kCapacity = 1024;
  using Queue = ds::MpscBoundedQueue<Message, kCapacity>;

  [[nodiscard]]
  MailboxPushResult Push(Message message) noexcept {
    const auto result = queue_.TryPush(message);

    if (result == ds::MpscQueuePushResult::kFull) {
      return MailboxPushResult::kFull;
    }

    if (notification_pending_.exchange(true, std::memory_order_acq_rel)) {
      return MailboxPushResult::kQueued;
    }

    return MailboxPushResult::kQueuedNeedsNotification;
  }

  [[nodiscard]]
  bool RetryNotification() noexcept {
    if (queue_.Empty()) {
      notification_pending_.store(false, std::memory_order_release);
      return false;
    }

    notification_pending_.store(true, std::memory_order_release);
    return true;
  }

  template <class F>
  std::size_t Drain(F&& handler) {
    std::size_t count = 0;

    for (;;) {
      const std::size_t drained = queue_.Drain([&](Message message) {
        handler(message);
        ++count;
      });

      if (drained != 0) {
        continue;
      }

      notification_pending_.store(false, std::memory_order_release);

      if (queue_.Empty()) {
        break;
      }

      bool expected = false;
      if (notification_pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                        std::memory_order_acquire)) {
        continue;
      }

      // Another producer owns the next notification.
      break;
    }

    return count;
  }

private:
  Queue queue_;
  std::atomic_bool notification_pending_{false};
};

}  // namespace coropact::luring::detail
