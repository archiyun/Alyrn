// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "coropact/ds/mpsc_bounded_queue.h"

namespace coropact::luring {

inline constexpr std::uint64_t kMsgRingNotificationUserData = 1;

struct LUringMessage {
  enum class Type : std::uint32_t {
    kResume = 1,
    kFunction = 2,
  };

  Type type{Type::kResume};
  std::uint64_t data{0};
};

enum class LUringMailboxPushResult : std::uint8_t {
  kQueuedNeedsNotification,
  kQueued,
  kFull,
};

class LUringMailbox {
public:
  static constexpr std::size_t kCapacity = 1024;
  using Queue = coropact::ds::MpscBoundedQueue<LUringMessage, kCapacity>;

  [[nodiscard]] LUringMailboxPushResult Push(
      LUringMessage message) noexcept {
    const auto result = queue_.TryPush(std::move(message));

    if (result == coropact::ds::MpscQueuePushResult::kFull) {
      return LUringMailboxPushResult::kFull;
    }

    if (notification_pending_.exchange(
            true,
            std::memory_order_acq_rel)) {
      return LUringMailboxPushResult::kQueued;
    }

    return LUringMailboxPushResult::kQueuedNeedsNotification;
  }

  [[nodiscard]] bool RetryNotification() noexcept {
    if (queue_.Empty()) {
      notification_pending_.store(
          false,
          std::memory_order_release);
      return false;
    }

    notification_pending_.store(
        true,
        std::memory_order_release);
    return true;
  }

  template <class F>
  std::size_t Drain(F&& handler) {
    std::size_t count = 0;

    for (;;) {
      const std::size_t drained = queue_.Drain(
          [&](LUringMessage message) {
            handler(message);
            ++count;
          });

      if (drained != 0) {
        continue;
      }

      notification_pending_.store(
          false,
          std::memory_order_release);

      if (queue_.Empty()) {
        break;
      }

      bool expected = false;
      if (notification_pending_.compare_exchange_strong(
              expected,
              true,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        continue;
      }

      // Another producer owns the next notification.
      break;
    }

    return count;
  }

  [[nodiscard]] std::size_t size() const noexcept {
    return queue_.Size();
  }

  [[nodiscard]] static constexpr std::size_t capacity() noexcept {
    return kCapacity;
  }

private:
  Queue queue_;
  std::atomic_bool notification_pending_{false};
};

}  // namespace coropact::luring
