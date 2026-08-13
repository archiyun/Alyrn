// SPDX-License-Identifier: MIT
#pragma once

#include <sys/event.h>

#include <cstddef>
#include <cstdint>
#include <vector>

/*
 * Control surface for the in-memory kqueue shim.
 *
 * The shim replaces kqueue() and kevent() so the kqueue backend's registration
 * state machine can be driven on a Linux host. It models the two behaviours
 * that the backend actually depends on and that no amount of reading can
 * confirm: EV_ADD replaces the flags of an existing (ident, filter) pair, and
 * a filter registered with EV_ONESHOT is dropped by the kernel at the moment
 * it is delivered.
 *
 * It is not a kqueue emulator. Readiness is injected by the test rather than
 * observed from real descriptors, so the shim proves what the backend asks the
 * kernel for and how it reacts to what it is handed back.
 */
namespace coropact::testing {

// One entry of what the shim currently holds, mirroring a kernel registration.
struct FakeKqueueFilter {
  int fd{-1};
  std::int16_t filter{0};
  std::uint16_t flags{0};
  void* udata{nullptr};
};

// One change the code under test submitted, in submission order.
struct FakeKqueueChange {
  int fd{-1};
  std::int16_t filter{0};
  std::uint16_t flags{0};
};

// Discards all registrations, pending readiness, and recorded changes. Call
// before constructing the loop under test.
void FakeKqueueReset();

// Queues readiness that the next kevent() wait will deliver. The filter must
// be registered by the time the wait runs, otherwise the trigger is dropped
// exactly as a real kernel would never report an unregistered filter.
void FakeKqueueTrigger(int fd, std::int16_t filter, std::uint16_t extra_flags = 0,
                       std::uint32_t fflags = 0);

[[nodiscard]]
bool FakeKqueueHasFilter(int fd, std::int16_t filter);

// Registered flags for one filter. Returns 0 when the filter is absent.
[[nodiscard]]
std::uint16_t FakeKqueueFilterFlags(int fd, std::int16_t filter);

[[nodiscard]]
std::vector<FakeKqueueFilter> FakeKqueueFilters();

[[nodiscard]]
const std::vector<FakeKqueueChange>& FakeKqueueChangeLog();

void FakeKqueueClearChangeLog();

// Counts submitted changes matching one filter and an exact flag word.
[[nodiscard]]
std::size_t FakeKqueueCountChanges(int fd, std::int16_t filter, std::uint16_t flags);

}  // namespace coropact::testing
