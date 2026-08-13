// SPDX-License-Identifier: MIT
#include "fake_kqueue.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <utility>

namespace coropact::testing {
namespace {

// A registration is keyed by (ident, filter), which is what makes read and
// write interest on one descriptor two independent entries.
using FilterKey = std::pair<int, std::int16_t>;

struct Registration {
  std::uint16_t flags{0};
  void* udata{nullptr};
};

struct PendingEvent {
  int fd{-1};
  std::int16_t filter{0};
  std::uint16_t extra_flags{0};
  std::uint32_t fflags{0};
};

struct FakeState {
  std::map<FilterKey, Registration> registrations;
  std::vector<PendingEvent> pending;
  std::vector<FakeKqueueChange> changes;

  /* The shim cannot block, so a loop that is never stopped would spin instead
   * of hanging on a real wait. Failing loudly after enough empty waits keeps
   * that mistake from costing a test run its timeout budget. */
  int empty_waits{0};
};

constexpr int kMaxEmptyWaits = 10000;

FakeState& State() {
  static FakeState state;
  return state;
}

int ApplyChange(const struct kevent& change) {
  const FilterKey key{static_cast<int>(change.ident), change.filter};
  FakeState& state = State();

  state.changes.push_back(FakeKqueueChange{key.first, key.second, change.flags});

  if (static_cast<bool>(change.flags & EV_DELETE)) {
    const auto entry = state.registrations.find(key);
    if (entry == state.registrations.end()) {
      errno = ENOENT;
      return -1;
    }
    state.registrations.erase(entry);

    // A deleted filter cannot still be on its way to userspace.
    std::erase_if(state.pending, [&key](const PendingEvent& pending) {
      return pending.fd == key.first && pending.filter == key.second;
    });
    return 0;
  }

  if (static_cast<bool>(change.flags & EV_ADD)) {
    // EV_ADD on an existing pair replaces its flags rather than duplicating
    // the entry. The backend relies on this to switch trigger modes.
    Registration& registration = state.registrations[key];
    registration.flags = change.flags;
    registration.udata = change.udata;
    return 0;
  }

  errno = EINVAL;
  return -1;
}

}  // namespace

void FakeKqueueReset() {
  FakeState& state = State();
  state.registrations.clear();
  state.pending.clear();
  state.changes.clear();
  state.empty_waits = 0;
}

void FakeKqueueTrigger(int fd, std::int16_t filter, std::uint16_t extra_flags,
                       std::uint32_t fflags) {
  State().pending.push_back(PendingEvent{fd, filter, extra_flags, fflags});
}

bool FakeKqueueHasFilter(int fd, std::int16_t filter) {
  const FakeState& state = State();
  return state.registrations.find(FilterKey{fd, filter}) != state.registrations.end();
}

std::uint16_t FakeKqueueFilterFlags(int fd, std::int16_t filter) {
  const FakeState& state = State();
  const auto entry = state.registrations.find(FilterKey{fd, filter});
  return entry == state.registrations.end() ? std::uint16_t{0} : entry->second.flags;
}

std::vector<FakeKqueueFilter> FakeKqueueFilters() {
  std::vector<FakeKqueueFilter> filters;
  for (const auto& [key, registration] : State().registrations) {
    filters.push_back(FakeKqueueFilter{key.first, key.second, registration.flags,
                                       registration.udata});
  }
  return filters;
}

const std::vector<FakeKqueueChange>& FakeKqueueChangeLog() { return State().changes; }

void FakeKqueueClearChangeLog() { State().changes.clear(); }

std::size_t FakeKqueueCountChanges(int fd, std::int16_t filter, std::uint16_t flags) {
  const std::vector<FakeKqueueChange>& changes = State().changes;
  return static_cast<std::size_t>(
      std::count_if(changes.begin(), changes.end(), [&](const FakeKqueueChange& change) {
        return change.fd == fd && change.filter == filter && change.flags == flags;
      }));
}

}  // namespace coropact::testing

extern "C" {

/*
 * A real descriptor is handed out so the loop under test can close() it and so
 * its number cannot collide with a descriptor the test opened itself.
 */
int kqueue(void) { return ::eventfd(0, EFD_CLOEXEC); }

int kevent(int kq, const struct kevent* changelist, int nchanges, struct kevent* eventlist,
           int nevents, const struct timespec* timeout) {
  (void)kq;
  (void)timeout;

  for (int i = 0; i < nchanges; ++i) {
    if (coropact::testing::ApplyChange(changelist[i]) != 0) {
      return -1;
    }
  }

  if (eventlist == nullptr || nevents <= 0) {
    return 0;
  }

  auto& state = coropact::testing::State();
  int produced = 0;

  auto pending = state.pending.begin();
  while (pending != state.pending.end() && produced < nevents) {
    const coropact::testing::FilterKey key{pending->fd, pending->filter};
    const auto entry = state.registrations.find(key);
    if (entry == state.registrations.end()) {
      // The kernel never reports a filter nobody registered.
      pending = state.pending.erase(pending);
      continue;
    }

    struct kevent& out = eventlist[produced];
    out.ident = static_cast<uintptr_t>(pending->fd);
    out.filter = pending->filter;
    out.flags = pending->extra_flags;
    out.fflags = pending->fflags;
    out.data = 0;
    // udata is captured at registration time, which is what lets the backend
    // recover its Channel from a returned event.
    out.udata = entry->second.udata;
    ++produced;

    // Delivery is what consumes a one-shot registration, not readiness.
    if (static_cast<bool>(entry->second.flags & EV_ONESHOT)) {
      state.registrations.erase(entry);
    }

    pending = state.pending.erase(pending);
  }

  if (produced == 0) {
    if (++state.empty_waits > coropact::testing::kMaxEmptyWaits) {
      std::fputs("fake kqueue: loop kept waiting with no readiness and no stop\n", stderr);
      std::abort();
    }
  } else {
    state.empty_waits = 0;
  }

  return produced;
}

}  // extern "C"
