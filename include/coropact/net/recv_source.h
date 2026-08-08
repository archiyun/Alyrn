// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/net/source_state.h"
#include "coropact/utils/macros.h"

namespace coropact::net {

// A move-only view of one backend-owned receive buffer. The reclaim callback
// returns the buffer to the backend pool/ring exactly once, when the lease is
// released or destroyed. A backend must keep the callback context alive until
// every outstanding lease has been released.
class BufferLease final {
public:
  COROPACT_DELETE_COPY(BufferLease);
  using ReclaimFn = void (*)(void* context, std::uint32_t buffer_id) noexcept;

  BufferLease() noexcept = default;

  BufferLease(std::byte* data, std::size_t size, std::uint32_t buffer_id, void* context,
              ReclaimFn reclaim) noexcept
      : data_(data), size_(size), buffer_id_(buffer_id), context_(context), reclaim_(reclaim) {}

  BufferLease(BufferLease&& other) noexcept
      : data_(std::exchange(other.data_, nullptr)),
        size_(std::exchange(other.size_, 0)),
        buffer_id_(std::exchange(other.buffer_id_, 0)),
        context_(std::exchange(other.context_, nullptr)),
        reclaim_(std::exchange(other.reclaim_, nullptr)) {}

  BufferLease& operator=(BufferLease&& other) noexcept {
    if (this != &other) {
      Release();
      data_ = std::exchange(other.data_, nullptr);
      size_ = std::exchange(other.size_, 0);
      buffer_id_ = std::exchange(other.buffer_id_, 0);
      context_ = std::exchange(other.context_, nullptr);
      reclaim_ = std::exchange(other.reclaim_, nullptr);
    }
    return *this;
  }

  ~BufferLease() noexcept { Release(); }

  [[nodiscard]]
  bool Valid() const noexcept {
    return reclaim_ != nullptr;
  }

  [[nodiscard]]
  std::span<const std::byte> Bytes() const noexcept {
    return {data_, size_};
  }

  [[nodiscard]]
  std::size_t Size() const noexcept {
    return size_;
  }

  [[nodiscard]]
  std::uint32_t BufferId() const noexcept {
    return buffer_id_;
  }

  // Idempotent. Moving a lease transfers the only reclaim obligation to the
  // destination; a moved-from lease is empty and does nothing on destruction.
  void Release() noexcept {
    if (reclaim_ != nullptr) {
      reclaim_(context_, buffer_id_);
    }
    data_ = nullptr;
    size_ = 0;
    buffer_id_ = 0;
    context_ = nullptr;
    reclaim_ = nullptr;
  }

private:
  std::byte* data_{nullptr};
  std::size_t size_{0};
  std::uint32_t buffer_id_{0};
  void* context_{nullptr};
  ReclaimFn reclaim_{nullptr};
};

struct RecvEvent {
  BufferLease buffer;
};

struct RecvSourceOptions {
  std::size_t pending_depth{1};
  std::size_t event_capacity{16};
  std::size_t buffer_capacity{16};
  // Zero selects event_capacity / 2. The threshold must stay below the
  // admission capacity so a paused source has room to re-arm safely.
  std::size_t resume_threshold{0};

  [[nodiscard]]
  constexpr bool Valid() const noexcept {
    return pending_depth > 0 && event_capacity > 0 && buffer_capacity >= event_capacity &&
           (resume_threshold == 0 || resume_threshold < event_capacity);
  }

  [[nodiscard]]
  constexpr std::size_t ResumeThreshold() const noexcept {
    return resume_threshold == 0 ? event_capacity / 2 : resume_threshold;
  }
};

namespace detail {

using RecvSourceState = SourceState;

// Backend-neutral admission and ownership accounting for a receive source.
// outstanding_leases counts both queued events and events already moved to a
// consumer: every produced buffer has a live BufferLease until its reclaim
// callback runs. The backend calls AcquireEvent() when it moves one queued
// RecvEvent to its consumer, and the corresponding BufferLease calls
// ReleaseLease() exactly once when it is destroyed.
//
// The owner must keep this state machine alive until OutstandingLeases() is
// zero. This is the lifetime boundary that lets a provided-buffer backend
// safely return a buffer after the coroutine has consumed the event.
class RecvSourceStateMachine final {
public:
  [[nodiscard]]
  static base::Result<RecvSourceStateMachine> Create(RecvSourceOptions options) noexcept {
    if (!options.Valid()) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }
    return RecvSourceStateMachine(options);
  }

  [[nodiscard]]
  base::Result<void> Start() noexcept {
    if (state_ != RecvSourceState::kIdle) {
      return std::unexpected(base::MakeErrno(EALREADY));
    }
    state_ = RecvSourceState::kActive;
    return {};
  }

  [[nodiscard]]
  bool TryArm() noexcept {
    if (!CanArm()) {
      return false;
    }
    ++armed_requests_;
    return true;
  }

  // Pausing is a recoverable admission state. The backend owns the physical
  // cancel/re-arm sequence; this machine only records the logical boundary.
  [[nodiscard]]
  base::Result<void> RequestPause() noexcept {
    if (state_ == RecvSourceState::kTerminal || state_ == RecvSourceState::kDraining ||
        state_ == RecvSourceState::kStopping || state_ == RecvSourceState::kPausing ||
        state_ == RecvSourceState::kPaused) {
      return {};
    }
    if (state_ != RecvSourceState::kActive) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }

    state_ = RecvSourceState::kPausing;
    ReconcilePause();
    return {};
  }

  [[nodiscard]]
  bool TryResume() noexcept {
    if (state_ != RecvSourceState::kPaused || queued_events_ > options_.ResumeThreshold()) {
      return false;
    }
    state_ = RecvSourceState::kActive;
    return true;
  }

  // F_MORE keeps the physical request armed. Only its terminal CQE releases
  // one armed request slot. A queued or directly delivered event consumes one
  // buffer slot until its lease is released.
  [[nodiscard]]
  base::Result<void> CompleteMultishotEvent(EventDisposition event,
                                            MultishotRequestDisposition request) noexcept {
    if (armed_requests_ == 0) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }
    if (event == EventDisposition::kProduced && !CanQueueEvent()) {
      return std::unexpected(base::MakeErrno(ENOBUFS));
    }
    if (event == EventDisposition::kDelivered && !CanDeliverEvent()) {
      return std::unexpected(base::MakeErrno(ENOBUFS));
    }

    if (request == MultishotRequestDisposition::kTerminal) {
      --armed_requests_;
    }
    if (event == EventDisposition::kProduced) {
      ++queued_events_;
    }
    if (event == EventDisposition::kProduced ||
        event == EventDisposition::kDelivered) {
      ++outstanding_leases_;
    }
    if (state_ != RecvSourceState::kActive) {
      ReconcileStopping();
    }
    return {};
  }

  // Transfers one queued event to the consumer and starts its lease lifetime.
  [[nodiscard]]
  bool AcquireEvent() noexcept {
    if (queued_events_ == 0) {
      return false;
    }
    --queued_events_;
    if (state_ != RecvSourceState::kActive) {
      ReconcileStopping();
    }
    return true;
  }

  // Rolls back the queue-side ownership after a backend failed to enqueue an
  // event. The BufferLease must already have returned its buffer, which has
  // decremented outstanding_leases_ separately.
  [[nodiscard]]
  bool DiscardQueuedEvent() noexcept {
    if (queued_events_ == 0) {
      return false;
    }
    --queued_events_;
    if (state_ != RecvSourceState::kActive) {
      ReconcileStopping();
    }
    return true;
  }

  [[nodiscard]]
  bool ReleaseLease() noexcept {
    if (outstanding_leases_ == 0) {
      return false;
    }
    --outstanding_leases_;
    if (state_ != RecvSourceState::kActive) {
      ReconcileStopping();
    }
    return true;
  }

  [[nodiscard]]
  base::Result<void> RequestStop() noexcept {
    if (state_ == RecvSourceState::kTerminal || state_ == RecvSourceState::kDraining ||
        state_ == RecvSourceState::kStopping) {
      return {};
    }

    if (state_ == RecvSourceState::kIdle) {
      state_ = RecvSourceState::kTerminal;
      return {};
    }

    state_ = RecvSourceState::kStopping;
    ReconcileStopping();
    return {};
  }

  [[nodiscard]]
  RecvSourceState State() const noexcept {
    return state_;
  }

  [[nodiscard]]
  const RecvSourceOptions& Options() const noexcept {
    return options_;
  }

  [[nodiscard]]
  std::size_t QueuedEvents() const noexcept {
    return queued_events_;
  }

  [[nodiscard]]
  std::size_t ArmedRequests() const noexcept {
    return armed_requests_;
  }

  [[nodiscard]]
  std::size_t OutstandingLeases() const noexcept {
    return outstanding_leases_;
  }

  [[nodiscard]]
  bool CanArm() const noexcept {
    return state_ == RecvSourceState::kActive && armed_requests_ < options_.pending_depth &&
           CanQueueEvent() && outstanding_leases_ + armed_requests_ < options_.buffer_capacity;
  }

  [[nodiscard]]
  bool CanQueueEvent() const noexcept {
    return queued_events_ < options_.event_capacity &&
           outstanding_leases_ < options_.buffer_capacity;
  }

private:
  [[nodiscard]]
  bool CanDeliverEvent() const noexcept {
    return outstanding_leases_ < options_.buffer_capacity;
  }

  explicit RecvSourceStateMachine(RecvSourceOptions options) noexcept : options_(options) {}

  void ReconcileStopping() noexcept {
    ReconcilePause();
    if (state_ == RecvSourceState::kStopping) {
      if (armed_requests_ != 0) {
        return;
      }
      state_ = queued_events_ == 0 && outstanding_leases_ == 0 ? RecvSourceState::kTerminal
                                                               : RecvSourceState::kDraining;
      return;
    }

    if (state_ == RecvSourceState::kDraining && armed_requests_ == 0 && queued_events_ == 0 &&
        outstanding_leases_ == 0) {
      state_ = RecvSourceState::kTerminal;
    }
  }

  void ReconcilePause() noexcept {
    if (state_ == RecvSourceState::kPausing && armed_requests_ == 0) {
      state_ = RecvSourceState::kPaused;
    }
  }

  RecvSourceOptions options_;
  RecvSourceState state_{RecvSourceState::kIdle};
  std::size_t queued_events_{0};
  std::size_t armed_requests_{0};
  std::size_t outstanding_leases_{0};
};

}  // namespace detail
}  // namespace coropact::net
