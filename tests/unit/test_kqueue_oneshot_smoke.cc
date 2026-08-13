// SPDX-License-Identifier: MIT

/*
 * Registration behaviour of TriggerMode::kOneShot, driven against the
 * in-memory kqueue shim.
 *
 * One-shot is the only mode whose kernel registration disappears without the
 * poller submitting anything, so the property under test throughout is that
 * the poller's mirror and the Channel's interest set stay truthful about a
 * filter the kernel has already dropped.
 */

#include <sys/eventfd.h>
#include <unistd.h>

#include <iostream>

#include "coropact/kqueue/detail/channel.h"
#include "coropact/kqueue/loop.h"
#include "coropact/kqueue/options.h"
#include "fake_kqueue.h"

namespace {

using coropact::kqueue::KqueueLoop;
using coropact::kqueue::TriggerMode;
using coropact::kqueue::detail::Channel;
using namespace coropact::testing;

constexpr std::uint16_t kOneShotArm = EV_ADD | EV_ENABLE | EV_ONESHOT;
constexpr std::uint16_t kLevelArm = EV_ADD | EV_ENABLE;

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

// A closeable descriptor to register interest against. Readiness is injected
// through the shim, so nothing is ever read from or written to it.
class ScopedFd {
public:
  ScopedFd() : fd_(::eventfd(0, EFD_CLOEXEC)) {}
  ~ScopedFd() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }
  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;

  [[nodiscard]] int Get() const { return fd_; }

private:
  int fd_;
};

struct Observation {
  int reads{0};
  int writes{0};
  int handled_turns{0};
};

/*
 * Runs the loop until a callback stops it. Every scenario below arms interest
 * and injects the matching readiness before calling this, so the first poll
 * has something to deliver.
 */
void RunUntilStopped(KqueueLoop& loop) { loop.Run(); }

bool CheckOneShotArmsWithOneShotFlag() {
  FakeKqueueReset();
  KqueueLoop loop;
  ScopedFd fd;
  Channel channel{&loop, fd.Get()};

  channel.SetTriggerMode(TriggerMode::kOneShot);
  channel.EnableReading();

  const bool armed = FakeKqueueFilterFlags(fd.Get(), EVFILT_READ) == kOneShotArm;
  channel.DisableAll();
  channel.Remove();

  return Check(armed, "one-shot interest must be registered with EV_ONESHOT");
}

/*
 * Delivery is what consumes a one-shot filter. Afterwards the kernel holds
 * nothing, and the Channel must agree: an interest set that still claimed the
 * read would make Remove() reject a Channel that is in fact already detached.
 */
bool CheckDeliveryRetiresRegistrationAndInterest() {
  FakeKqueueReset();
  Observation observation;

  KqueueLoop loop;
  ScopedFd fd;
  Channel channel{&loop, fd.Get()};

  struct Context {
    Observation* observation;
    KqueueLoop* loop;
  } context{&observation, &loop};

  channel.SetTriggerMode(TriggerMode::kOneShot);
  channel.SetReadCallback(
      [](void* raw) noexcept {
        auto* ctx = static_cast<Context*>(raw);
        ++ctx->observation->reads;
        ctx->loop->RequestStop();
      },
      &context);
  channel.EnableReading();

  FakeKqueueTrigger(fd.Get(), EVFILT_READ);
  RunUntilStopped(loop);

  const bool fired = observation.reads == 1;
  const bool kernel_dropped = !FakeKqueueHasFilter(fd.Get(), EVFILT_READ);
  const bool interest_cleared = channel.IsNoneEvent();

  // Remove() requires an empty interest set, so reaching it without an
  // intervening DisableAll() is itself part of the contract being checked.
  channel.Remove();

  return Check(fired, "one-shot read must dispatch its callback") &&
         Check(kernel_dropped, "one-shot delivery must leave no kernel registration") &&
         Check(interest_cleared, "one-shot delivery must clear the Channel's interest");
}

/*
 * Re-arming is the whole point of the mode. It only works if the poller's
 * mirror was cleared on delivery, otherwise EnableReading() looks like a
 * no-op change and the second EV_ADD is never submitted.
 */
bool CheckReArmSubmitsFreshRegistration() {
  FakeKqueueReset();
  Observation observation;

  KqueueLoop loop;
  ScopedFd fd;
  Channel channel{&loop, fd.Get()};

  struct Context {
    Observation* observation;
    KqueueLoop* loop;
    Channel* channel;
    int fd;
  } context{&observation, &loop, &channel, fd.Get()};

  channel.SetTriggerMode(TriggerMode::kOneShot);
  channel.SetReadCallback(
      [](void* raw) noexcept {
        auto* ctx = static_cast<Context*>(raw);
        ++ctx->observation->reads;
        if (ctx->observation->reads == 1) {
          ctx->channel->EnableReading();
          FakeKqueueTrigger(ctx->fd, EVFILT_READ);
          return;
        }
        ctx->loop->RequestStop();
      },
      &context);
  channel.EnableReading();

  FakeKqueueTrigger(fd.Get(), EVFILT_READ);
  RunUntilStopped(loop);

  const bool fired_twice = observation.reads == 2;
  const bool armed_twice = FakeKqueueCountChanges(fd.Get(), EVFILT_READ, kOneShotArm) == 2;

  channel.DisableAll();
  channel.Remove();

  return Check(fired_twice, "a re-armed one-shot read must fire again") &&
         Check(armed_twice, "re-arming must submit a second EV_ADD for the filter");
}

/*
 * A retired filter is already gone from the kernel, so submitting EV_DELETE
 * for it would be rejected with ENOENT. The shim would surface that as a
 * failed change, but the mirror is what has to prevent it being sent at all.
 */
bool CheckRetiredFilterIsNotDeleted() {
  FakeKqueueReset();
  Observation observation;

  KqueueLoop loop;
  ScopedFd fd;
  Channel channel{&loop, fd.Get()};

  struct Context {
    Observation* observation;
    KqueueLoop* loop;
  } context{&observation, &loop};

  channel.SetTriggerMode(TriggerMode::kOneShot);
  channel.SetReadCallback(
      [](void* raw) noexcept {
        auto* ctx = static_cast<Context*>(raw);
        ++ctx->observation->reads;
        ctx->loop->RequestStop();
      },
      &context);
  channel.EnableReading();

  FakeKqueueTrigger(fd.Get(), EVFILT_READ);
  RunUntilStopped(loop);

  FakeKqueueClearChangeLog();
  channel.DisableAll();
  channel.Remove();

  const bool no_delete = FakeKqueueCountChanges(fd.Get(), EVFILT_READ, EV_DELETE) == 0;
  return Check(no_delete, "a retired one-shot filter must not be deleted again");
}

/*
 * Contrast case. A level-triggered registration survives delivery, which is
 * what makes the one-shot retirement above a real behavioural difference
 * rather than an artefact of the shim.
 */
bool CheckLevelTriggeredSurvivesDelivery() {
  FakeKqueueReset();
  Observation observation;

  KqueueLoop loop;
  ScopedFd fd;
  Channel channel{&loop, fd.Get()};

  struct Context {
    Observation* observation;
    KqueueLoop* loop;
  } context{&observation, &loop};

  channel.SetReadCallback(
      [](void* raw) noexcept {
        auto* ctx = static_cast<Context*>(raw);
        ++ctx->observation->reads;
        ctx->loop->RequestStop();
      },
      &context);
  channel.EnableReading();

  const bool armed_level = FakeKqueueFilterFlags(fd.Get(), EVFILT_READ) == kLevelArm;

  FakeKqueueTrigger(fd.Get(), EVFILT_READ);
  RunUntilStopped(loop);

  const bool still_registered = FakeKqueueHasFilter(fd.Get(), EVFILT_READ);
  const bool interest_kept = channel.IsReading();

  channel.DisableAll();
  channel.Remove();

  return Check(armed_level, "level-triggered interest must register without EV_ONESHOT") &&
         Check(still_registered, "level-triggered delivery must keep the registration") &&
         Check(interest_kept, "level-triggered delivery must keep the Channel's interest");
}

/*
 * Read and write are two registrations, so both can be delivered in one wait.
 * The Channel must still be dispatched once, with both filters retired.
 */
bool CheckBothFiltersRetireInOneTurn() {
  FakeKqueueReset();
  Observation observation;

  KqueueLoop loop;
  ScopedFd fd;
  Channel channel{&loop, fd.Get()};

  struct Context {
    Observation* observation;
    KqueueLoop* loop;
  } context{&observation, &loop};

  channel.SetTriggerMode(TriggerMode::kOneShot);
  channel.SetReadCallback(
      [](void* raw) noexcept { ++static_cast<Context*>(raw)->observation->reads; }, &context);
  channel.SetWriteCallback(
      [](void* raw) noexcept {
        auto* ctx = static_cast<Context*>(raw);
        ++ctx->observation->writes;
        ctx->loop->RequestStop();
      },
      &context);
  channel.EnableReading();
  channel.EnableWriting();

  FakeKqueueTrigger(fd.Get(), EVFILT_READ);
  FakeKqueueTrigger(fd.Get(), EVFILT_WRITE);
  RunUntilStopped(loop);

  const bool read_once = observation.reads == 1;
  const bool write_once = observation.writes == 1;
  const bool both_dropped = !FakeKqueueHasFilter(fd.Get(), EVFILT_READ) &&
                            !FakeKqueueHasFilter(fd.Get(), EVFILT_WRITE);
  const bool interest_cleared = channel.IsNoneEvent();

  channel.Remove();

  return Check(read_once, "a merged turn must run the read callback exactly once") &&
         Check(write_once, "a merged turn must run the write callback exactly once") &&
         Check(both_dropped, "both one-shot filters must be retired in one turn") &&
         Check(interest_cleared, "a merged turn must clear both interest bits");
}

/*
 * Switching mode while armed is expressed as a re-add, because EV_ADD on an
 * existing pair replaces its flags rather than creating a second entry.
 */
bool CheckModeSwitchReArmsInPlace() {
  FakeKqueueReset();
  KqueueLoop loop;
  ScopedFd fd;
  Channel channel{&loop, fd.Get()};

  channel.EnableReading();
  const bool started_level = FakeKqueueFilterFlags(fd.Get(), EVFILT_READ) == kLevelArm;

  channel.SetTriggerMode(TriggerMode::kOneShot);
  const bool switched = FakeKqueueFilterFlags(fd.Get(), EVFILT_READ) == kOneShotArm;

  std::size_t read_filters = 0;
  for (const FakeKqueueFilter& filter : FakeKqueueFilters()) {
    if (filter.fd == fd.Get() && filter.filter == EVFILT_READ) {
      ++read_filters;
    }
  }

  channel.DisableAll();
  channel.Remove();

  return Check(started_level, "the Channel must start level-triggered") &&
         Check(switched, "switching to one-shot while armed must re-add the filter") &&
         Check(read_filters == 1, "a mode switch must not leave a duplicate registration");
}

}  // namespace

int main() {
  if (!CheckOneShotArmsWithOneShotFlag()) return 1;
  if (!CheckDeliveryRetiresRegistrationAndInterest()) return 1;
  if (!CheckReArmSubmitsFreshRegistration()) return 1;
  if (!CheckRetiredFilterIsNotDeleted()) return 1;
  if (!CheckLevelTriggeredSurvivesDelivery()) return 1;
  if (!CheckBothFiltersRetireInOneTurn()) return 1;
  if (!CheckModeSwitchReArmsInPlace()) return 1;
  std::cout << "kqueue one-shot smoke: PASS\n";
  return 0;
}
