// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <thread>

#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/io/loop.h"
#include "coropact/luring/connector.h"
#include "coropact/luring/detail/loop_access.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/options.h"
#include "coropact/luring/timer.h"

namespace {

using namespace std::chrono_literals;

static_assert(requires(coropact::luring::LUringConnector& connector) { connector.SleepFor(1ms); });

bool Check(bool condition, const char* message) {
  if (!condition) std::cout << "FAIL: " << message << '\n';
  return condition;
}

bool ExpectChildAbort(void (*entry)(), const char* message) {
  const pid_t child = ::fork();
  if (child < 0) {
    return Check(false, "fork failed for luring loop affinity test");
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    entry();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Check(WIFSIGNALED(status), message) &&
         Check(WTERMSIG(status) == SIGABRT, "loop-affinity invariant must terminate with SIGABRT");
}

void RunAfterFromForeignThread() {
  coropact::luring::LUringLoop loop;
  std::thread foreign([&loop] { (void)loop.RunAfter(0ms, [] noexcept {}); });
  foreign.join();
}

void DestroyLoopFromForeignThread() {
  auto* loop = new coropact::luring::LUringLoop;
  std::thread foreign([loop] { delete loop; });
  foreign.join();
}

bool TestLoopAffinityIsEnforcedInRelease() {
  return ExpectChildAbort(&RunAfterFromForeignThread,
                          "RunAfter from a foreign thread must terminate in Release") &&
         ExpectChildAbort(&DestroyLoopFromForeignThread,
                          "LUringLoop destruction from a foreign thread must terminate in Release");
}

bool IsEnvironmentSkip(coropact::base::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

bool StopAndDrain(coropact::luring::LUringLoop& loop) {
  loop.RequestStop();
  loop.Run();

  return Check(loop.State() == coropact::io::LoopState::kStopped,
               "manual timer loop cleanup should stop the loop") &&
         Check(coropact::luring::detail::LoopAccess::IsDrained(loop),
               "manual timer loop cleanup should drain user operation work");
}

coropact::coro::DetachedTask SleepTask(coropact::luring::LUringLoop* loop, bool* resumed,
                                       bool* scheduler_ok) {
  auto result = co_await coropact::luring::SleepFor(*loop, 1ms);
  *resumed = true;
  *scheduler_ok = coropact::coro::Scheduler::TryCurrent() == loop;
  if (!result.has_value()) co_return;
}

bool TestTimers() {
  coropact::luring::LUringLoop loop;
  coropact::luring::LUringOptions options;
  options.entries = 16;
  options.submit_batch = 1;

  auto init = loop.Init(options);
  if (!init.has_value()) {
    if (IsEnvironmentSkip(init.error())) {
      std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
      return true;
    }
    return Check(false, "LUringLoop initialization failed");
  }

  bool early_fired = false;
  bool late_fired = false;
  auto late = loop.RunAfter(100ms, [&late_fired] noexcept { late_fired = true; });
  if (!Check(late.has_value(), "late timer should be accepted")) {
    (void)StopAndDrain(loop);
    return false;
  }

  auto early = loop.RunAfter(2ms, [&early_fired] noexcept { early_fired = true; });
  if (!Check(early.has_value(), "early timer should be accepted")) {
    (void)StopAndDrain(loop);
    return false;
  }

  // Updating an already armed timeout may produce one or more control CQEs
  // before the updated timer itself expires.
  while (!early_fired && !late_fired) {
    auto completed = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!Check(completed.has_value(), "timer completion should be received")) {
      (void)StopAndDrain(loop);
      return false;
    }
  }

  if (!Check(early_fired, "earlier timer should fire first") ||
      !Check(!late_fired, "later timer should not fire early")) {
    (void)StopAndDrain(loop);
    return false;
  }

  if (!Check(loop.CancelTimer(*late).has_value(), "later timer should be cancellable")) {
    (void)StopAndDrain(loop);
    return false;
  }

  bool resumed = false;
  bool scheduler_ok = false;
  coropact::coro::SpawnDetach(loop, SleepTask(&loop, &resumed, &scheduler_ok));
  coropact::luring::detail::LoopAccess::RunReady(loop);
  auto completed = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  coropact::luring::detail::LoopAccess::RunReady(loop);

  const bool passed = Check(completed.has_value(), "sleep should complete") &&
                      Check(resumed, "SleepFor should resume the coroutine") &&
                      Check(scheduler_ok, "SleepFor should resume on its loop scheduler");
  return StopAndDrain(loop) && passed;
}

bool TestStopDiscardsUnexpiredTimer() {
  bool fired = false;
  {
    coropact::luring::LUringLoop loop;
    coropact::luring::LUringOptions options;
    options.entries = 8;
    options.submit_batch = 1;

    auto init = loop.Init(options);
    if (!init.has_value()) {
      if (IsEnvironmentSkip(init.error())) {
        return true;
      }
      return Check(false, "LUringLoop initialization failed for stop test");
    }

    auto timer = loop.RunAfter(1h, [&fired] noexcept { fired = true; });
    if (!Check(timer.has_value(), "unexpired timer should be accepted")) {
      return false;
    }

    std::jthread stopper([&loop] {
      std::this_thread::sleep_for(2ms);
      loop.RequestStop();
    });
    loop.Run();
    stopper.join();

    if (!Check(loop.State() == coropact::io::LoopState::kStopped,
               "loop with an unexpired timer should stop")) {
      return false;
    }
  }

  return Check(!fired, "loop shutdown must discard an unexpired timer without running it");
}

bool TestRunAfterRejectsTimerPreparationFailure() {
  coropact::luring::LUringLoop loop;
  coropact::luring::LUringOptions options;
  options.entries = 8;
  options.submit_batch = 1;

  auto init = loop.Init(options);
  if (!init.has_value()) {
    if (IsEnvironmentSkip(init.error())) {
      return true;
    }
    return Check(false, "LUringLoop initialization failed for timer preparation test");
  }

  loop.FailNextSubmissionsForTesting(1, EIO);
  auto timer = loop.RunAfter(1ms, [] noexcept {});

  const bool rejected =
      Check(!timer.has_value(), "RunAfter must reject timer driver preparation failure") &&
      Check(timer.error().value() == EIO, "RunAfter must preserve timer preparation error");
  return StopAndDrain(loop) && rejected;
}

bool TestRunAfterRejectsTimerUpdatePreparationFailure() {
  coropact::luring::LUringLoop loop;
  coropact::luring::LUringOptions options;
  options.entries = 8;
  options.submit_batch = 1;

  auto init = loop.Init(options);
  if (!init.has_value()) {
    if (IsEnvironmentSkip(init.error())) {
      return true;
    }
    return Check(false, "LUringLoop initialization failed for timer update test");
  }

  auto later = loop.RunAfter(1h, [] noexcept {});
  if (!Check(later.has_value(), "initial timer should be accepted before update failure")) {
    (void)StopAndDrain(loop);
    return false;
  }

  loop.FailNextSubmissionsForTesting(1, EIO);
  auto earlier = loop.RunAfter(1ms, [] noexcept {});

  const bool rejected =
      Check(!earlier.has_value(), "RunAfter must reject timer update preparation failure") &&
      Check(earlier.error().value() == EIO, "RunAfter must preserve timer update error");
  const bool original_retained = Check(loop.CancelTimer(*later).has_value(),
                                       "failed earlier timer must not remove prior timer");
  const bool drained = StopAndDrain(loop);
  return rejected && original_retained && drained;
}

bool TestTimerRearmFailureStopsLoop() {
  coropact::luring::LUringLoop loop;
  coropact::luring::LUringOptions options;
  options.entries = 8;
  options.submit_batch = 1;

  auto init = loop.Init(options);
  if (!init.has_value()) {
    if (IsEnvironmentSkip(init.error())) {
      return true;
    }
    return Check(false, "LUringLoop initialization failed for timer rearm test");
  }

  bool first_fired = false;
  auto first = loop.RunAfter(1ms, [&first_fired] noexcept { first_fired = true; });
  if (!Check(first.has_value(), "initial expiring timer should be accepted")) {
    (void)StopAndDrain(loop);
    return false;
  }
  auto later = loop.RunAfter(1h, [] noexcept {});
  if (!Check(later.has_value(), "later timer should be accepted before rearm failure")) {
    (void)StopAndDrain(loop);
    return false;
  }

  loop.FailNextSubmissionsForTesting(1, EIO);
  auto completed = coropact::luring::detail::LoopAccess::WaitCompletions(loop);

  const bool failed_safe =
      Check(completed.has_value(), "expiring timer completion should be received") &&
      Check(first_fired, "first timer should fire before rearm failure") &&
      Check(loop.State() == coropact::io::LoopState::kStopping,
            "timer rearm preparation failure must stop the loop");
  const bool drained = StopAndDrain(loop);
  return failed_safe && drained;
}

}  // namespace

int main() {
  if (!TestLoopAffinityIsEnforcedInRelease()) return 1;
  if (!TestTimers()) return 1;
  if (!TestStopDiscardsUnexpiredTimer()) return 1;
  if (!TestRunAfterRejectsTimerPreparationFailure()) return 1;
  if (!TestRunAfterRejectsTimerUpdatePreparationFailure()) return 1;
  if (!TestTimerRearmFailureStopsLoop()) return 1;
  std::cout << "luring timer smoke: PASS\n";
  return 0;
}
