// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <array>
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>

#include "coropact/utils/macros.h"

namespace coropact::ds {

enum class MpscQueuePushResult : std::uint8_t {
  kPushed,
  kFull,
};

// Bounded multi-producer queue with one intended consumer.
//
// The sequence number in each cell separates slot reservation from slot
// publication. Producers reserve a position with CAS, construct the object,
// and then publish the cell with release semantics. The consumer only reads a
// cell after observing that publication with acquire semantics.
template <class T, std::size_t Capacity>
class MpscBoundedQueue {
public:
  COROPACT_DELETE_COPY_MOVE(MpscBoundedQueue);

  static_assert(Capacity > 0);
  static_assert((Capacity & (Capacity - 1)) == 0,
                "Capacity must be a power of two");
  static_assert(Capacity <= std::numeric_limits<std::size_t>::max() / 2);
  static_assert(std::is_nothrow_destructible_v<T>);
  static_assert(std::is_nothrow_move_constructible_v<T>);

  MpscBoundedQueue() noexcept {
    for (std::size_t i = 0; i < Capacity; ++i) {
      buffer_[i].sequence.store(i, std::memory_order_relaxed);
    }
  }

  // All producers must stop before destruction begins.
  ~MpscBoundedQueue() noexcept {
    while (TryPop().has_value()) {
    }
  }

  template <class U>
    requires std::constructible_from<T, U&&> &&
             std::is_nothrow_constructible_v<T, U&&>
  [[nodiscard]] MpscQueuePushResult TryPush(U&& value) noexcept {
    constexpr std::size_t mask = Capacity - 1;

    std::size_t position =
        enqueue_position_.load(std::memory_order_relaxed);
    Cell* cell = nullptr;

    for (;;) {
      cell = &buffer_[position & mask];

      const std::size_t sequence =
          cell->sequence.load(std::memory_order_acquire);
      const auto difference =
          static_cast<std::intptr_t>(sequence) -
          static_cast<std::intptr_t>(position);

      if (difference == 0) {
        if (enqueue_position_.compare_exchange_weak(
                position,
                position + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) {
          break;
        }
      } else if (difference < 0) {
        return MpscQueuePushResult::kFull;
      } else {
        position = enqueue_position_.load(std::memory_order_relaxed);
      }
    }

    std::construct_at(cell->Value(), std::forward<U>(value));
    cell->sequence.store(position + 1, std::memory_order_release);
    return MpscQueuePushResult::kPushed;
  }

  [[nodiscard]] std::optional<T> TryPop() noexcept {
    constexpr std::size_t mask = Capacity - 1;

    std::size_t position =
        dequeue_position_.load(std::memory_order_relaxed);
    Cell* cell = nullptr;

    for (;;) {
      cell = &buffer_[position & mask];

      const std::size_t sequence =
          cell->sequence.load(std::memory_order_acquire);
      const auto difference =
          static_cast<std::intptr_t>(sequence) -
          static_cast<std::intptr_t>(position + 1);

      if (difference == 0) {
        if (dequeue_position_.compare_exchange_weak(
                position,
                position + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) {
          break;
        }
      } else if (difference < 0) {
        return std::nullopt;
      } else {
        position = dequeue_position_.load(std::memory_order_relaxed);
      }
    }

    std::optional<T> result{
        std::in_place,
        std::move(*cell->Value())};

    std::destroy_at(cell->Value());
    cell->sequence.store(position + Capacity, std::memory_order_release);
    return result;
  }

  template <class F>
  std::size_t Drain(F&& handler) {
    std::size_t count = 0;

    while (auto value = TryPop()) {
      handler(std::move(*value));
      ++count;
    }

    return count;
  }

  // These values are snapshots and may change immediately after returning.
  [[nodiscard]] bool Empty() const noexcept {
    return enqueue_position_.load(std::memory_order_acquire) ==
           dequeue_position_.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::size_t Size() const noexcept {
    return enqueue_position_.load(std::memory_order_acquire) -
           dequeue_position_.load(std::memory_order_acquire);
  }

  [[nodiscard]] static constexpr std::size_t CapacityValue() noexcept {
    return Capacity;
  }

private:
  struct Cell {
    std::atomic<std::size_t> sequence{0};
    alignas(T) std::byte storage[sizeof(T)];

    [[nodiscard]] T* Value() noexcept {
      return std::launder(reinterpret_cast<T*>(storage));
    }
  };

  alignas(64) std::atomic<std::size_t> enqueue_position_{0};
  alignas(64) std::atomic<std::size_t> dequeue_position_{0};
  std::array<Cell, Capacity> buffer_{};
};

}  // namespace coropact::ds
