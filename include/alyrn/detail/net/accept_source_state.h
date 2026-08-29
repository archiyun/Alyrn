// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <cstddef>

#include "alyrn/net/accept_source.h"
#include "alyrn/detail/net/source_state.h"
#include "alyrn/result.h"

namespace alyrn::net::detail {

using AcceptSourceState = SourceState;

/*
 * Backend-neutral accept admission state. It owns no Stream values and does
 * no scheduling; an adapter accounts for physical requests in its completion
 * path. Pause first blocks admission, then waits for already armed requests
 * to drain before it becomes observable as paused.
 */
class AcceptSourceStateMachine final {
public:
  static Result<AcceptSourceStateMachine> Create(AcceptSourceOptions options) noexcept {
    if (!options.Valid()) {
      return std::unexpected(Errno(EINVAL));
    }
    return AcceptSourceStateMachine(options);
  }

  Result<void> Start() noexcept {
    if (state_ != AcceptSourceState::kIdle) {
      return std::unexpected(Errno(EALREADY));
    }
    state_ = AcceptSourceState::kActive;
    return {};
  }

  bool TryArm() noexcept {
    if (state_ != AcceptSourceState::kActive || armed_requests_ >= options_.pending_depth ||
        queued_events_ + armed_requests_ >= options_.event_capacity) {
      return false;
    }
    ++armed_requests_;
    return true;
  }

  Result<void> RequestPause() noexcept {
    if (state_ == AcceptSourceState::kTerminal || state_ == AcceptSourceState::kDraining ||
        state_ == AcceptSourceState::kStopping || state_ == AcceptSourceState::kPausing ||
        state_ == AcceptSourceState::kPaused) {
      return {};
    }
    if (state_ != AcceptSourceState::kActive) {
      return std::unexpected(Errno(EINVAL));
    }

    state_ = AcceptSourceState::kPausing;
    ReconcilePause();
    return {};
  }

  bool TryResume() noexcept {
    if (state_ != AcceptSourceState::kPaused || queued_events_ > options_.ResumeThreshold()) {
      return false;
    }
    state_ = AcceptSourceState::kActive;
    return true;
  }

  Result<void> CompleteRequest(bool produced_event) noexcept {
    if (armed_requests_ == 0) {
      return std::unexpected(Errno(EINVAL));
    }
    if (produced_event && queued_events_ >= options_.event_capacity) {
      return std::unexpected(Errno(ENOBUFS));
    }

    --armed_requests_;
    if (produced_event) {
      ++queued_events_;
    }
    ReconcileStopping();
    return {};
  }

  Result<void> CompleteMultishotEvent(EventDisposition event,
                                      MultishotRequestDisposition request) noexcept {
    if (armed_requests_ == 0) {
      return std::unexpected(Errno(EINVAL));
    }
    if (event == EventDisposition::kProduced && queued_events_ >= options_.event_capacity) {
      return std::unexpected(Errno(ENOBUFS));
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

  bool ConsumeEvent() noexcept {
    if (queued_events_ == 0) {
      return false;
    }
    --queued_events_;
    ReconcileStopping();
    return true;
  }

  void RequestStop() noexcept {
    if (state_ == AcceptSourceState::kTerminal || state_ == AcceptSourceState::kDraining ||
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

  AcceptSourceState State() const noexcept {
    return state_;
  }

  const AcceptSourceOptions& Options() const noexcept {
    return options_;
  }

  std::size_t QueuedEvents() const noexcept {
    return queued_events_;
  }

  std::size_t ArmedRequests() const noexcept {
    return armed_requests_;
  }

  bool CanArm() const noexcept {
    return state_ == AcceptSourceState::kActive && armed_requests_ < options_.pending_depth &&
           queued_events_ + armed_requests_ < options_.event_capacity;
  }

private:
  explicit AcceptSourceStateMachine(AcceptSourceOptions options) noexcept : options_(options) {}

  void ReconcileStopping() noexcept {
    ReconcilePause();
    if (state_ == AcceptSourceState::kStopping) {
      if (armed_requests_ != 0) {
        return;
      }
      state_ = queued_events_ == 0 ? AcceptSourceState::kTerminal : AcceptSourceState::kDraining;
      return;
    }

    if (state_ == AcceptSourceState::kDraining && queued_events_ == 0 && armed_requests_ == 0) {
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

}  // namespace alyrn::net::detail
