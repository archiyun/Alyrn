// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <cassert>
#include <cerrno>
#include <utility>

#include "coropact/net/accept_source.h"

namespace {

using coropact::net::AcceptSourceOptions;
using coropact::net::detail::AcceptSourceState;
using coropact::net::detail::AcceptSourceStateMachine;

void CheckOptions() {
  assert(!AcceptSourceOptions{0, 1}.Valid());
  assert(!AcceptSourceOptions{4, 3}.Valid());
  assert(AcceptSourceOptions{4, 4}.Valid());

  auto invalid = AcceptSourceStateMachine::Create({0, 1});
  assert(!invalid.has_value());
  assert(invalid.error().value() == EINVAL);
}

void CheckAdmissionBudget() {
  auto machine_result = AcceptSourceStateMachine::Create({2, 3});
  assert(machine_result.has_value());
  auto machine = std::move(*machine_result);

  assert(machine.Start().has_value());
  assert(machine.State() == AcceptSourceState::kActive);

  assert(machine.TryArm());
  assert(machine.TryArm());
  assert(!machine.TryArm());
  assert(machine.ArmedRequests() == 2);

  assert(machine.CompleteRequest(true).has_value());
  assert(machine.QueuedEvents() == 1);
  assert(machine.ArmedRequests() == 1);

  // queued(1) + armed(1) leaves one admission slot.
  assert(machine.TryArm());
  assert(!machine.TryArm());
  assert(machine.QueuedEvents() + machine.ArmedRequests() == 3);

  assert(machine.ConsumeEvent());
  assert(machine.QueuedEvents() == 0);
  assert(machine.ConsumeEvent() == false);
}

void CheckStopDrainsQueuedEvents() {
  auto machine_result = AcceptSourceStateMachine::Create({1, 2});
  assert(machine_result.has_value());
  auto machine = std::move(*machine_result);

  assert(machine.Start().has_value());
  assert(machine.TryArm());
  assert(machine.CompleteRequest(true).has_value());
  assert(machine.QueuedEvents() == 1);

  assert(machine.RequestStop().has_value());
  assert(machine.State() == AcceptSourceState::kDraining);
  assert(!machine.TryArm());

  assert(machine.ConsumeEvent());
  assert(machine.State() == AcceptSourceState::kTerminal);
  assert(!machine.ConsumeEvent());
  assert(machine.RequestStop().has_value());
}

void CheckStopWaitsForPendingRequests() {
  auto machine_result = AcceptSourceStateMachine::Create({2, 2});
  assert(machine_result.has_value());
  auto machine = std::move(*machine_result);

  assert(machine.Start().has_value());
  assert(machine.TryArm());
  assert(machine.TryArm());
  assert(machine.RequestStop().has_value());
  assert(machine.State() == AcceptSourceState::kStopping);

  assert(machine.CompleteRequest(false).has_value());
  assert(machine.State() == AcceptSourceState::kStopping);
  assert(machine.ArmedRequests() == 1);

  assert(machine.CompleteRequest(false).has_value());
  assert(machine.State() == AcceptSourceState::kTerminal);
  assert(machine.ArmedRequests() == 0);
}

void CheckInvalidCompletion() {
  auto machine_result = AcceptSourceStateMachine::Create({1, 1});
  assert(machine_result.has_value());
  auto machine = std::move(*machine_result);

  assert(machine.Start().has_value());
  auto completion = machine.CompleteRequest(true);
  assert(!completion.has_value());
  assert(completion.error().value() == EINVAL);
}

}  // namespace

int main() {
  CheckOptions();
  CheckAdmissionBudget();
  CheckStopDrainsQueuedEvents();
  CheckStopWaitsForPendingRequests();
  CheckInvalidCompletion();
  return 0;
}
