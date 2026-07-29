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
using coropact::net::detail::EventDisposition;
using coropact::net::detail::MultishotRequestDisposition;

void CheckOptions() {
  assert((!AcceptSourceOptions{0, 1}.Valid()));
  assert((!AcceptSourceOptions{4, 3}.Valid()));
  assert((AcceptSourceOptions{4, 4}.Valid()));
  assert((AcceptSourceOptions{1, 1}.Valid()));

  auto invalid = AcceptSourceStateMachine::Create({0, 1});
  assert(!invalid.has_value());
  assert(invalid.error().value() == EINVAL);

  invalid = AcceptSourceStateMachine::Create({4, 3});
  assert(!invalid.has_value());
  assert(invalid.error().value() == EINVAL);
}

void CheckStartAndStopEdges() {
  auto machine_result = AcceptSourceStateMachine::Create({1, 1});
  assert(machine_result.has_value());
  auto machine = std::move(*machine_result);

  // An idle source can be stopped without ever arming a backend request.
  assert(machine.RequestStop().has_value());
  assert(machine.State() == AcceptSourceState::kTerminal);
  assert(machine.RequestStop().has_value());
  assert(!machine.TryArm());

  auto started = AcceptSourceStateMachine::Create({1, 1});
  assert(started.has_value());
  auto active = std::move(*started);
  assert(active.Start().has_value());
  auto second_start = active.Start();
  assert(!second_start.has_value());
  assert(second_start.error().value() == EALREADY);
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

  assert(machine.TryArm());
  assert(machine.CompleteRequest(false).has_value());
  auto duplicate = machine.CompleteRequest(false);
  assert(!duplicate.has_value());
  assert(duplicate.error().value() == EINVAL);
}

void CheckMultishotCapacityAndTransientCompletion() {
  auto machine_result = AcceptSourceStateMachine::Create({1, 2});
  assert(machine_result.has_value());
  auto machine = std::move(*machine_result);

  assert(machine.Start().has_value());
  assert(machine.TryArm());

  // A non-event F_MORE CQE is still part of the same physical request.
  assert(machine.CompleteMultishotEvent(
      EventDisposition::kNone,
      MultishotRequestDisposition::kMore).has_value());
  assert(machine.ArmedRequests() == 1);
  assert(machine.QueuedEvents() == 0);

  assert(machine.CompleteMultishotEvent(
      EventDisposition::kProduced,
      MultishotRequestDisposition::kMore).has_value());
  assert(machine.CompleteMultishotEvent(
      EventDisposition::kProduced,
      MultishotRequestDisposition::kMore).has_value());
  assert(machine.QueuedEvents() == 2);
  assert(machine.ArmedRequests() == 1);

  // Backpressure must not silently consume the physical request or enqueue
  // an event beyond the configured capacity.
  auto full = machine.CompleteMultishotEvent(
      EventDisposition::kProduced,
      MultishotRequestDisposition::kMore);
  assert(!full.has_value());
  assert(full.error().value() == ENOBUFS);
  assert(machine.QueuedEvents() == 2);
  assert(machine.ArmedRequests() == 1);

  // A terminal CQE is still accepted after the queue has filled; the backend
  // is responsible for having requested cancellation when it saw ENOBUFS.
  assert(machine.CompleteMultishotEvent(
      EventDisposition::kNone,
      MultishotRequestDisposition::kTerminal).has_value());
  assert(machine.ArmedRequests() == 0);
  assert(machine.State() == AcceptSourceState::kActive);

  assert(machine.ConsumeEvent());
  assert(machine.ConsumeEvent());
  assert(machine.State() == AcceptSourceState::kActive);
  assert(machine.RequestStop().has_value());
  assert(machine.State() == AcceptSourceState::kTerminal);
}

void CheckMultishotTerminalEventAndDuplicateTerminal() {
  auto machine_result = AcceptSourceStateMachine::Create({1, 1});
  assert(machine_result.has_value());
  auto machine = std::move(*machine_result);

  assert(machine.Start().has_value());
  assert(machine.TryArm());

  // A terminal error has no queued event but still releases the request.
  assert(machine.CompleteMultishotEvent(
      EventDisposition::kNone,
      MultishotRequestDisposition::kTerminal).has_value());
  assert(machine.State() == AcceptSourceState::kActive);
  assert(machine.ArmedRequests() == 0);

  assert(machine.RequestStop().has_value());
  assert(machine.State() == AcceptSourceState::kTerminal);

  auto duplicate = machine.CompleteMultishotEvent(
      EventDisposition::kNone,
      MultishotRequestDisposition::kTerminal);
  assert(!duplicate.has_value());
  assert(duplicate.error().value() == EINVAL);
}

void CheckMultishotLifecycle() {
  auto machine_result = AcceptSourceStateMachine::Create({1, 3});
  assert(machine_result.has_value());
  auto machine = std::move(*machine_result);

  assert(machine.Start().has_value());
  assert(machine.TryArm());

  // F_MORE keeps the one physical request armed while producing events.
  assert(machine.CompleteMultishotEvent(
      EventDisposition::kProduced,
      MultishotRequestDisposition::kMore).has_value());
  assert(machine.ArmedRequests() == 1);
  assert(machine.QueuedEvents() == 1);

  assert(machine.CompleteMultishotEvent(
      EventDisposition::kProduced,
      MultishotRequestDisposition::kMore).has_value());
  assert(machine.ArmedRequests() == 1);
  assert(machine.QueuedEvents() == 2);

  // The terminal CQE releases the physical request.
  assert(machine.CompleteMultishotEvent(
      EventDisposition::kNone,
      MultishotRequestDisposition::kTerminal).has_value());
  assert(machine.ArmedRequests() == 0);
  assert(machine.State() == AcceptSourceState::kActive);

  assert(machine.ConsumeEvent());
  assert(machine.ConsumeEvent());
  assert(!machine.ConsumeEvent());
}

void CheckMultishotStopDrain() {
  auto machine_result = AcceptSourceStateMachine::Create({1, 2});
  assert(machine_result.has_value());
  auto machine = std::move(*machine_result);

  assert(machine.Start().has_value());
  assert(machine.TryArm());
  assert(machine.RequestStop().has_value());
  assert(machine.State() == AcceptSourceState::kStopping);

  assert(machine.CompleteMultishotEvent(
      EventDisposition::kProduced,
      MultishotRequestDisposition::kMore).has_value());
  assert(machine.State() == AcceptSourceState::kStopping);
  assert(machine.ArmedRequests() == 1);

  assert(machine.CompleteMultishotEvent(
      EventDisposition::kNone,
      MultishotRequestDisposition::kTerminal).has_value());
  assert(machine.State() == AcceptSourceState::kDraining);
  assert(machine.ArmedRequests() == 0);

  assert(machine.ConsumeEvent());
  assert(machine.State() == AcceptSourceState::kTerminal);
}

}  // namespace

int main() {
  CheckOptions();
  CheckStartAndStopEdges();
  CheckAdmissionBudget();
  CheckStopDrainsQueuedEvents();
  CheckStopWaitsForPendingRequests();
  CheckInvalidCompletion();
  CheckMultishotCapacityAndTransientCompletion();
  CheckMultishotTerminalEventAndDuplicateTerminal();
  CheckMultishotLifecycle();
  CheckMultishotStopDrain();
  return 0;
}
