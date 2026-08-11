// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <cstddef>

#include "coropact/base/error.h"
#include "coropact/net/accept_source.h"
#include "coropact/net/detail/source_state.h"

namespace coropact::net::detail {

using AcceptSourceState = SourceState;

// Backend-neutral lifecycle and admission state. It owns no Stream values and
// performs no scheduling; a backend stores one instance in its AcceptSource
// and calls CompleteRequest() from its completion path.
class AcceptSourceStateMachine final {
public:
  [[nodiscard]]
  static base::Result<AcceptSourceStateMachine> Create(AcceptSourceOptions options) noexcept {
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

  [[nodiscard]]
  bool TryArm() noexcept {
    if (state_ != AcceptSourceState::kActive || armed_requests_ >= options_.pending_depth ||
        queued_events_ + armed_requests_ >= options_.event_capacity) {
      return false;
    }
    ++armed_requests_;
    return true;
  }

  [[nodiscard]]
  base::Result<void> RequestPause() noexcept {
    if (state_ == AcceptSourceState::kTerminal || state_ == AcceptSourceState::kDraining ||
        state_ == AcceptSourceState::kStopping || state_ == AcceptSourceState::kPausing ||
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

  [[nodiscard]]
  bool TryResume() noexcept {
    if (state_ != AcceptSourceState::kPaused || queued_events_ > options_.ResumeThreshold()) {
      return false;
    }
    state_ = AcceptSourceState::kActive;
    return true;
  }

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

  [[nodiscard]]
  base::Result<void> CompleteMultishotEvent(EventDisposition event,
                                            MultishotRequestDisposition request) noexcept {
    if (armed_requests_ == 0) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }
    if (event == EventDisposition::kProduced && queued_events_ >= options_.event_capacity) {
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

  [[nodiscard]]
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

  [[nodiscard]]
  AcceptSourceState State() const noexcept {
    return state_;
  }

  [[nodiscard]]
  const AcceptSourceOptions& Options() const noexcept {
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

}  // namespace coropact::net::detail
