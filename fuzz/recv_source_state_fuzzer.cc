// SPDX-License-Identifier: MIT

#include <cstddef>
#include <cstdint>

#include "alyrn/detail/check.h"
#include "alyrn/net/detail/recv_source_state.h"

namespace {

using alyrn::net::RecvSourceOptions;
using alyrn::net::detail::EventDisposition;
using alyrn::net::detail::MultishotRequestDisposition;
using alyrn::net::detail::RecvSourceState;
using alyrn::net::detail::RecvSourceStateMachine;

void CheckInvariants(const RecvSourceStateMachine& state) {
  const auto& options = state.Options();
  ALYRN_CHECK(state.QueuedEvents() <= options.event_capacity,
                 "receive source queue exceeded its configured capacity");
  ALYRN_CHECK(state.OutstandingLeases() <= options.buffer_capacity,
                 "receive source leases exceeded its configured capacity");
  ALYRN_CHECK(state.ArmedRequests() <= options.pending_depth,
                 "receive source armed too many physical requests");
  if (state.State() == RecvSourceState::kTerminal) {
    ALYRN_CHECK(state.ArmedRequests() == 0 && state.QueuedEvents() == 0 &&
                       state.OutstandingLeases() == 0,
                   "terminal receive source retained observable work");
  }
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size < 3) return 0;

  RecvSourceOptions options{
      .pending_depth = static_cast<std::size_t>(data[0] % 4 + 1),
      .event_capacity = static_cast<std::size_t>(data[1] % 4 + 1),
      .buffer_capacity = static_cast<std::size_t>(data[1] % 4 + 1 + data[2] % 4),
  };
  auto created = RecvSourceStateMachine::Create(options);
  if (!created.has_value()) return 0;
  auto state = std::move(*created);

  for (std::size_t index = 3; index < size; ++index) {
    const std::uint8_t operation = data[index];
    switch (operation % 8) {
      case 0:
        (void)state.Start();
        break;
      case 1:
        (void)state.TryArm();
        break;
      case 2: {
        const auto event = static_cast<EventDisposition>((operation >> 3) % 3);
        const auto request = static_cast<MultishotRequestDisposition>((operation >> 5) % 2);
        (void)state.CompleteMultishotEvent(event, request);
        break;
      }
      case 3:
        (void)state.AcquireEvent();
        break;
      case 4:
        (void)state.DiscardQueuedEvent();
        break;
      case 5:
        (void)state.ReleaseLease();
        break;
      case 6:
        (void)state.RequestPause();
        break;
      case 7:
        if ((operation & 0x80U) != 0) {
          (void)state.RequestStop();
        } else {
          (void)state.TryResume();
        }
        break;
    }
    CheckInvariants(state);
  }
  return 0;
}
