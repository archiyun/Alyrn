// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>

#include "coropact/base/error.h"
#include "coropact/net/source_state.h"

namespace coropact::net {

// Admission and buffering limits shared by Reactor and luring AcceptSource
// implementations. pending_depth limits physical accept requests; the event
// capacity limits accepted Stream values that can wait for the consumer.
struct AcceptSourceOptions {
  std::size_t pending_depth{4};
  std::size_t event_capacity{64};
  // Zero selects event_capacity / 2. The threshold must stay below the
  // admission capacity so a paused source has room to re-arm safely.
  std::size_t resume_threshold{0};

  [[nodiscard]]
  constexpr bool Valid() const noexcept {
    return pending_depth > 0 && event_capacity >= pending_depth &&
           (resume_threshold == 0 || resume_threshold < event_capacity);
  }

  [[nodiscard]]
  constexpr std::size_t ResumeThreshold() const noexcept {
    return resume_threshold == 0 ? event_capacity / 2 : resume_threshold;
  }
};

namespace detail {

using AcceptSourceState = SourceState;

// Backend-neutral lifecycle and admission state. It owns no Stream values and
// performs no scheduling; a backend stores one instance in its AcceptSource
// and calls CompleteRequest() from its completion path.
class AcceptSourceStateMachine final {
public:
  [[nodiscard]]
  static base::Result<AcceptSourceStateMachine> Create(
      AcceptSourceOptions options) noexcept {
    if (!options.Valid()) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }
    return AcceptSourceStateMachine(options);
  }

  [[nodiscard]]
  base::Result<void> Start() noexcept {
    if (state_ != AcceptSourceState::kIdle) {
      return std::unexpected(base::MakeErrno(EALREADY));
    }
    state_ = AcceptSourceState::kActive;
    return {};
  }

  // Returns false for normal backpressure or a stopped source. It never
  // blocks and does not submit a backend operation by itself.
  [[nodiscard]]
  bool TryArm() noexcept {
    if (state_ != AcceptSourceState::kActive ||
        armed_requests_ >= options_.pending_depth ||
        queued_events_ + armed_requests_ >= options_.event_capacity) {
      return false;
    }
    ++armed_requests_;
    return true;
  }

  // Backpressure pauses admission without making the logical source
  // terminal. The backend must cancel the active physical request and feed
  // its terminal CQE back through CompleteMultishotEvent/CompleteRequest.
  [[nodiscard]]
  base::Result<void> RequestPause() noexcept {
    if (state_ == AcceptSourceState::kTerminal ||
        state_ == AcceptSourceState::kDraining ||
        state_ == AcceptSourceState::kStopping ||
        state_ == AcceptSourceState::kPausing ||
        state_ == AcceptSourceState::kPaused) {
      return {};
    }
    if (state_ != AcceptSourceState::kActive) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }

    state_ = AcceptSourceState::kPausing;
    ReconcilePause();
    return {};
  }

  // Returns true only when a paused source crossed its low-water mark and
  // can accept a new physical request.
  [[nodiscard]]
  bool TryResume() noexcept {
    if (state_ != AcceptSourceState::kPaused ||
        queued_events_ > options_.ResumeThreshold()) {
      return false;
    }
    state_ = AcceptSourceState::kActive;
    return true;
  }

  // Records one physical accept completion. produced_event must be true only
  // when the completion transfers an accepted Stream into the source queue.
  [[nodiscard]]
  base::Result<void> CompleteRequest(bool produced_event) noexcept {
    if (armed_requests_ == 0) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }
    if (produced_event && queued_events_ >= options_.event_capacity) {
      return std::unexpected(base::MakeErrno(ENOBUFS));
    }

    --armed_requests_;
    if (produced_event) {
      ++queued_events_;
    }
    ReconcileStopping();
    return {};
  }

  // Records one CQE from a multi-shot request. F_MORE keeps the physical
  // request armed; only its terminal CQE releases the armed request slot.
  [[nodiscard]]
  base::Result<void> CompleteMultishotEvent(
      EventDisposition event,
      MultishotRequestDisposition request) noexcept {
    if (armed_requests_ == 0) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }
    if (event == EventDisposition::kProduced &&
        queued_events_ >= options_.event_capacity) {
      return std::unexpected(base::MakeErrno(ENOBUFS));
    }

    if (request == MultishotRequestDisposition::kTerminal) {
      --armed_requests_;
    }
    if (event == EventDisposition::kProduced) {
      ++queued_events_;
    }
    ReconcileStopping();
    return {};
  }

  // Consumes one already queued accepted Stream. The backend performs the
  // actual Stream move; this method only advances admission state.
  [[nodiscard]]
  bool ConsumeEvent() noexcept {
    if (queued_events_ == 0) {
      return false;
    }
    --queued_events_;
    ReconcileStopping();
    return true;
  }

  // Stop is idempotent. Existing queued events remain available to Next();
  // after pending requests drain and the queue is consumed, the source is
  // terminal and Next() returns the normal end result.
  void RequestStop() noexcept {
    if (state_ == AcceptSourceState::kTerminal ||
        state_ == AcceptSourceState::kDraining ||
        state_ == AcceptSourceState::kStopping) {
      return;
    }

    if (state_ == AcceptSourceState::kIdle) {
      state_ = AcceptSourceState::kTerminal;
      return;
    }

    state_ = AcceptSourceState::kStopping;
    ReconcileStopping();
  }

  [[nodiscard]]
  AcceptSourceState State() const noexcept { return state_; }

  [[nodiscard]]
  const AcceptSourceOptions& Options() const noexcept { return options_; }

  [[nodiscard]]
  std::size_t QueuedEvents() const noexcept { return queued_events_; }

  [[nodiscard]]
  std::size_t ArmedRequests() const noexcept { return armed_requests_; }

  [[nodiscard]]
  bool CanArm() const noexcept {
    return state_ == AcceptSourceState::kActive &&
           armed_requests_ < options_.pending_depth &&
           queued_events_ + armed_requests_ < options_.event_capacity;
  }

private:
  explicit AcceptSourceStateMachine(AcceptSourceOptions options) noexcept
      : options_(options) {}

  void ReconcileStopping() noexcept {
    ReconcilePause();
    if (state_ == AcceptSourceState::kStopping) {
      if (armed_requests_ != 0) {
        return;
      }
      state_ = queued_events_ == 0 ? AcceptSourceState::kTerminal
                                   : AcceptSourceState::kDraining;
      return;
    }

    if (state_ == AcceptSourceState::kDraining && queued_events_ == 0 &&
        armed_requests_ == 0) {
      state_ = AcceptSourceState::kTerminal;
    }
  }

  void ReconcilePause() noexcept {
    if (state_ == AcceptSourceState::kPausing && armed_requests_ == 0) {
      state_ = AcceptSourceState::kPaused;
    }
  }

  AcceptSourceOptions options_;
  AcceptSourceState state_{AcceptSourceState::kIdle};
  std::size_t queued_events_{0};
  std::size_t armed_requests_{0};
};

}  // namespace detail
}  // namespace coropact::net
