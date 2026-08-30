// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <cerrno>
#include <cstddef>

#include "alyrn/net/detail/source_state.h"
#include "alyrn/net/recv_source.h"
#include "alyrn/result.h"

namespace alyrn::net::detail {

using RecvSourceState = SourceState;

/*
 * Backend-neutral receive admission and lease accounting. The owner keeps this
 * state alive until OutstandingLeases() is zero so Stop() can wait for every
 * BufferLease the application still holds.
 *
 * kMaxRearmWorkingSet is the kernel provided-buffer pipeline depth. Copy-out
 * adapters recycle slots during CQE handling; publishing more than this only
 * lengthens the ENOBUFS burst before userspace can restock the ring.
 */
class RecvSourceStateMachine final {
public:
  static constexpr std::size_t kMaxRearmWorkingSet = 8;
  static Result<RecvSourceStateMachine> Create(RecvSourceOptions options) noexcept {
    if (!options.Valid()) {
      return std::unexpected(Errno(EINVAL));
    }
    return RecvSourceStateMachine(options);
  }

  Result<void> Start() noexcept {
    if (state_ != RecvSourceState::kIdle) {
      return std::unexpected(Errno(EALREADY));
    }
    state_ = RecvSourceState::kActive;
    return {};
  }

  bool TryArm() noexcept {
    if (!CanArm()) {
      return false;
    }
    ++armed_requests_;
    return true;
  }

  Result<void> RequestPause() noexcept {
    if (state_ == RecvSourceState::kTerminal || state_ == RecvSourceState::kDraining ||
        state_ == RecvSourceState::kStopping || state_ == RecvSourceState::kPausing ||
        state_ == RecvSourceState::kPaused) {
      return {};
    }
    if (state_ != RecvSourceState::kActive) {
      return std::unexpected(Errno(EINVAL));
    }

    state_ = RecvSourceState::kPausing;
    ReconcilePause();
    return {};
  }

  bool TryResume() noexcept {
    if (state_ != RecvSourceState::kPaused || queued_events_ > options_.ResumeThreshold()) {
      return false;
    }
    state_ = RecvSourceState::kActive;
    return true;
  }

  Result<void> CompleteMultishotEvent(EventDisposition event,
                                      MultishotRequestDisposition request) noexcept {
    if (armed_requests_ == 0) {
      return std::unexpected(Errno(EINVAL));
    }
    if (event == EventDisposition::kProduced && !CanQueueEvent()) {
      return std::unexpected(Errno(ENOBUFS));
    }
    if (event == EventDisposition::kDelivered && !CanDeliverEvent()) {
      return std::unexpected(Errno(ENOBUFS));
    }

    if (request == MultishotRequestDisposition::kTerminal) {
      --armed_requests_;
    }
    if (event == EventDisposition::kProduced) {
      ++queued_events_;
    }
    if (event == EventDisposition::kProduced || event == EventDisposition::kDelivered) {
      ++outstanding_leases_;
    }
    if (state_ != RecvSourceState::kActive) {
      ReconcileStopping();
    }
    return {};
  }

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

  Result<void> RequestStop() noexcept {
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

  RecvSourceState State() const noexcept { return state_; }

  const RecvSourceOptions& Options() const noexcept { return options_; }

  std::size_t QueuedEvents() const noexcept { return queued_events_; }

  std::size_t ArmedRequests() const noexcept { return armed_requests_; }

  std::size_t OutstandingLeases() const noexcept { return outstanding_leases_; }

  bool CanArm() const noexcept {
    if (state_ != RecvSourceState::kActive || armed_requests_ >= options_.pending_depth ||
        !CanQueueEvent()) {
      return false;
    }
    const std::size_t reserved = outstanding_leases_ + armed_requests_;
    if (reserved >= options_.buffer_capacity) {
      return false;
    }
    const std::size_t free_slots = options_.buffer_capacity - reserved;
    const std::size_t min_free = std::min(options_.ResumeThreshold(), kMaxRearmWorkingSet);
    return free_slots >= min_free;
  }

  bool CanQueueEvent() const noexcept {
    return queued_events_ < options_.event_capacity &&
           outstanding_leases_ < options_.buffer_capacity;
  }

private:
  bool CanDeliverEvent() const noexcept { return outstanding_leases_ < options_.buffer_capacity; }

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

}  // namespace alyrn::net::detail
