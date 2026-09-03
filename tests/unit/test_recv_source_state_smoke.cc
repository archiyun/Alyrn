// SPDX-License-Identifier: MIT

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <utility>

#include "alyrn/result.h"
#include "alyrn/coro/task.h"
#include "alyrn/io/recv_source.h"
#include "alyrn/net/recv_source.h"

namespace {

using alyrn::Result;
using alyrn::coro::Task;
using alyrn::io::AsyncRecvSource;
using alyrn::net::BufferLease;
using alyrn::net::RecvEvent;
using alyrn::net::RecvSourceOptions;
using alyrn::net::detail::EventDisposition;
using alyrn::net::detail::MultishotRequestDisposition;
using alyrn::net::detail::RecvSourceState;
using alyrn::net::detail::RecvSourceStateMachine;

class ContractOnlyRecvSource final {
public:
  using Event = RecvEvent;

  Task<Result<std::optional<Event>>> Next();
  Result<void> RequestStop() noexcept;
  Task<Result<void>> Stop();
};

static_assert(AsyncRecvSource<ContractOnlyRecvSource>);

struct ReclaimObservation {
  int count{0};
  std::uint32_t last_id{0};
};

void Reclaim(void* context, std::uint32_t buffer_id) noexcept {
  auto* observation = static_cast<ReclaimObservation*>(context);
  ++observation->count;
  observation->last_id = buffer_id;
}

struct ReentrantReclaimObservation {
  BufferLease* lease{nullptr};
  int count{0};
  std::uint32_t last_id{0};
  bool saw_released_lease{false};
};

void ReentrantReclaim(void* context, std::uint32_t buffer_id) noexcept {
  auto* observation = static_cast<ReentrantReclaimObservation*>(context);
  ++observation->count;
  observation->last_id = buffer_id;
  observation->saw_released_lease = !observation->lease->Valid() &&
                                    observation->lease->Bytes().empty() &&
                                    observation->lease->BufferId() == 0;
  if (observation->count == 1) {
    observation->lease->Release();
  }
}

struct StateReclaimContext {
  RecvSourceStateMachine* state{nullptr};
  int count{0};
};

void ReleaseStateLease(void* context, std::uint32_t /*buffer_id*/) noexcept {
  auto* reclaim = static_cast<StateReclaimContext*>(context);
  assert(reclaim->state->ReleaseLease());
  ++reclaim->count;
}

void CheckOptions() {
  assert((!RecvSourceOptions{0, 1, 1}.Valid()));
  assert((!RecvSourceOptions{1, 0, 1}.Valid()));
  assert((!RecvSourceOptions{1, 2, 1}.Valid()));
  assert((RecvSourceOptions{1, 2, 2}.Valid()));
  assert((!RecvSourceOptions{1, 2, 2, 2}.Valid()));
  assert((RecvSourceOptions{1, 2, 2}.ResumeThreshold() == 1));
}

void CheckBufferLease() {
  std::array<std::byte, 8> storage{};
  ReclaimObservation observation;

  BufferLease moved;
  {
    BufferLease lease(storage.data(), storage.size(), 7, &observation, &Reclaim);
    assert(lease.Valid());
    assert(lease.Size() == storage.size());
    assert(lease.BufferId() == 7);
    assert(lease.Bytes().data() == storage.data());

    moved = std::move(lease);
    assert(!lease.Valid());
    assert(moved.Valid());
    assert(observation.count == 0);
  }

  assert(observation.count == 0);
  moved.Release();
  assert(!moved.Valid());
  assert(observation.count == 1);
  assert(observation.last_id == 7);

  moved.Release();
  assert(observation.count == 1);
}

void CheckBufferLeaseReleasesBeforeReclaim() {
  std::array<std::byte, 8> storage{};
  ReentrantReclaimObservation observation;
  BufferLease lease(storage.data(), storage.size(), 11, &observation, &ReentrantReclaim);
  observation.lease = &lease;

  lease.Release();

  assert(!lease.Valid());
  assert(lease.Bytes().empty());
  assert(lease.BufferId() == 0);
  assert(observation.count == 1);
  assert(observation.last_id == 11);
  assert(observation.saw_released_lease);
}

void CheckLeaseLifetimeAndStop() {
  auto state_result = RecvSourceStateMachine::Create({
      .pending_depth = 1,
      .event_capacity = 2,
      .buffer_capacity = 2,
  });
  assert(state_result.has_value());
  auto state = std::move(*state_result);

  assert(state.Start().has_value());
  assert(state.TryArm());

  // An F_MORE event keeps the physical request armed and queues one buffer.
  assert(state.CompleteMultishotEvent(
                 EventDisposition::kProduced,
                 MultishotRequestDisposition::kMore)
             .has_value());
  assert(state.ArmedRequests() == 1);
  assert(state.QueuedEvents() == 1);
  assert(state.OutstandingLeases() == 1);

  StateReclaimContext reclaim{.state = &state};
  assert(state.AcquireEvent());
  assert(state.QueuedEvents() == 0);
  assert(state.OutstandingLeases() == 1);

  // The terminal CQE produces a second event. Both buffer slots are now held
  // by the consumer/queue, so the backend must not arm another request yet.
  assert(state.CompleteMultishotEvent(
                 EventDisposition::kProduced,
                 MultishotRequestDisposition::kTerminal)
             .has_value());
  assert(state.ArmedRequests() == 0);
  assert(state.QueuedEvents() == 1);
  assert(state.OutstandingLeases() == 2);
  assert(!state.CanArm());

  auto stopped = state.RequestStop();
  assert(stopped.has_value());
  assert(state.State() == RecvSourceState::kDraining);

  // Stop drains queued events but cannot become terminal until both leases
  // have been returned.
  assert(state.AcquireEvent());
  assert(state.OutstandingLeases() == 2);
  assert(state.State() == RecvSourceState::kDraining);

  std::array<std::byte, 1> storage{};
  {
    BufferLease first(storage.data(), storage.size(), 1, &reclaim,
                      &ReleaseStateLease);
    BufferLease second(storage.data(), storage.size(), 2, &reclaim,
                       &ReleaseStateLease);
    assert(reclaim.count == 0);
  }
  assert(reclaim.count == 2);
  assert(state.OutstandingLeases() == 0);
  assert(state.State() == RecvSourceState::kTerminal);
}

void CheckBufferCapacityFailure() {
  auto state_result = RecvSourceStateMachine::Create({
      .pending_depth = 1,
      .event_capacity = 1,
      .buffer_capacity = 1,
  });
  assert(state_result.has_value());
  auto state = std::move(*state_result);

  assert(state.Start().has_value());
  assert(state.TryArm());
  assert(state.CompleteMultishotEvent(
                 EventDisposition::kProduced,
                 MultishotRequestDisposition::kMore)
             .has_value());

  // A second event cannot be represented while the only provided buffer is
  // already owned. The physical request remains armed so the backend can
  // cancel it and converge through the normal terminal path.
  auto overflow = state.CompleteMultishotEvent(
      EventDisposition::kProduced, MultishotRequestDisposition::kMore);
  assert(!overflow.has_value());
  assert(overflow.Error().value() == ENOBUFS);
  assert(state.State() == RecvSourceState::kActive);
  assert(state.ArmedRequests() == 1);
  assert(state.QueuedEvents() == 1);

  assert(state.CompleteMultishotEvent(
                 EventDisposition::kNone,
                 MultishotRequestDisposition::kTerminal)
             .has_value());
  assert(state.RequestStop().has_value());
  assert(state.State() == RecvSourceState::kDraining);
  assert(state.AcquireEvent());
  assert(state.RequestStop().has_value());
  assert(state.State() == RecvSourceState::kDraining);
  assert(state.ReleaseLease());
  assert(state.State() == RecvSourceState::kTerminal);
}

void CheckDirectDeliveryAccounting() {
  auto state_result = RecvSourceStateMachine::Create({
      .pending_depth = 1,
      .event_capacity = 2,
      .buffer_capacity = 2,
  });
  assert(state_result.has_value());
  auto state = std::move(*state_result);

  assert(state.Start().has_value());
  assert(state.TryArm());
  assert(state.CompleteMultishotEvent(
                 EventDisposition::kDelivered,
                 MultishotRequestDisposition::kMore)
             .has_value());
  assert(state.ArmedRequests() == 1);
  assert(state.QueuedEvents() == 0);
  assert(state.OutstandingLeases() == 1);
  assert(!state.AcquireEvent());

  // A directly delivered lease still keeps Stop in the draining state until
  // the consumer releases it.
  assert(state.RequestStop().has_value());
  assert(state.CompleteMultishotEvent(
                 EventDisposition::kNone,
                 MultishotRequestDisposition::kTerminal)
             .has_value());
  assert(state.State() == RecvSourceState::kDraining);
  assert(state.ReleaseLease());
  assert(state.State() == RecvSourceState::kTerminal);
}

void CheckPauseAndResume() {
  auto state_result = RecvSourceStateMachine::Create({
      .pending_depth = 1,
      .event_capacity = 2,
      .buffer_capacity = 2,
      .resume_threshold = 0,
  });
  assert(state_result.has_value());
  auto state = std::move(*state_result);

  assert(state.Start().has_value());
  assert(state.TryArm());
  assert(state.CompleteMultishotEvent(
                 EventDisposition::kProduced,
                 MultishotRequestDisposition::kMore)
             .has_value());

  assert(state.RequestPause().has_value());
  assert(state.State() == RecvSourceState::kPausing);
  assert(state.CompleteMultishotEvent(
                 EventDisposition::kNone,
                 MultishotRequestDisposition::kTerminal)
             .has_value());
  assert(state.State() == RecvSourceState::kPaused);

  assert(state.AcquireEvent());
  assert(state.TryResume());
  assert(state.State() == RecvSourceState::kActive);
}

}  // namespace

int main() {
  CheckOptions();
  CheckBufferLease();
  CheckBufferLeaseReleasesBeforeReclaim();
  CheckLeaseLifetimeAndStop();
  CheckBufferCapacityFailure();
  CheckDirectDeliveryAccounting();
  CheckPauseAndResume();
  std::cout << "recv source/BufferLease state smoke: PASS\n";
  return 0;
}
