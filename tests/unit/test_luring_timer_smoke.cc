// SPDX-License-Identifier: MIT

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <thread>

#include "alyrn/coro/spawn.h"
#include "alyrn/io/loop.h"
#include "alyrn/uring/connector.h"
#include "alyrn/uring/detail/loop_access.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/options.h"
#include "alyrn/uring/timer.h"

namespace {

using namespace std::chrono_literals;

static_assert(requires(alyrn::uring::Connector& connector) { connector.SleepFor(1ms); });

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
  alyrn::uring::Loop loop;
  std::thread foreign([&loop] { (void)loop.RunAfter(0ms, [] noexcept {}); });
  foreign.join();
}

void DestroyLoopFromForeignThread() {
  auto* loop = new alyrn::uring::Loop;
  std::thread foreign([loop] { delete loop; });
  foreign.join();
}

bool TestLoopAffinityIsEnforcedInRelease() {
  return ExpectChildAbort(&RunAfterFromForeignThread,
                          "RunAfter from a foreign thread must terminate in Release") &&
         ExpectChildAbort(&DestroyLoopFromForeignThread,
                          "Loop destruction from a foreign thread must terminate in Release");
}

bool IsEnvironmentSkip(alyrn::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

bool StopAndDrain(alyrn::uring::Loop& loop) {
  loop.RequestStop();
  loop.Run();

  return Check(loop.State() == alyrn::io::LoopState::kStopped,
               "manual timer loop cleanup should stop the loop") &&
         Check(alyrn::uring::detail::LoopAccess::IsDrained(loop),
               "manual timer loop cleanup should drain user operation work");
}

alyrn::coro::DetachedTask SleepTask(alyrn::uring::Loop* loop, bool* resumed,
                                       bool* scheduler_ok) {
  auto result = co_await alyrn::uring::SleepFor(*loop, 1ms);
  *resumed = true;
  *scheduler_ok = alyrn::coro::Scheduler::TryCurrent() == loop;
  if (!result.HasValue()) co_return;
}

bool TestTimers() {
  alyrn::uring::Loop loop;
  alyrn::uring::Options options;
  options.entries = 16;

  auto init = loop.Init(options);
  if (!init.HasValue()) {
    if (IsEnvironmentSkip(init.Error())) {
      std::cout << "SKIP: io_uring unavailable: " << init.Error().message() << '\n';
      return true;
    }
    return Check(false, "Loop initialization failed");
  }

  bool early_fired = false;
  bool late_fired = false;
  auto late = loop.RunAfter(100ms, [&late_fired] noexcept { late_fired = true; });
  if (!Check(late.HasValue(), "late timer should be accepted")) {
    (void)StopAndDrain(loop);
    return false;
  }

  auto early = loop.RunAfter(2ms, [&early_fired] noexcept { early_fired = true; });
  if (!Check(early.HasValue(), "early timer should be accepted")) {
    (void)StopAndDrain(loop);
    return false;
  }

  // Updating an already armed timeout may produce one or more control CQEs
  // before the updated timer itself expires.
  while (!early_fired && !late_fired) {
    auto completed = alyrn::uring::detail::LoopAccess::WaitCompletions(loop);
    if (!Check(completed.HasValue(), "timer completion should be received")) {
      (void)StopAndDrain(loop);
      return false;
    }
  }

  if (!Check(early_fired, "earlier timer should fire first") ||
      !Check(!late_fired, "later timer should not fire early")) {
    (void)StopAndDrain(loop);
    return false;
  }

  if (!Check(loop.CancelTimer(*late).HasValue(), "later timer should be cancellable")) {
    (void)StopAndDrain(loop);
    return false;
  }

  bool resumed = false;
  bool scheduler_ok = false;
  alyrn::coro::SpawnDetach(loop, SleepTask(&loop, &resumed, &scheduler_ok));
  alyrn::uring::detail::LoopAccess::RunReady(loop);

  // A successful timeout update may retire the replaced physical request with
  // an ECANCELED CQE before the re-armed driver reaches ETIME. Keep driving
  // physical completions until the logical SleepFor continuation is ready.
  bool completion_received = false;
  for (int attempt = 0; attempt != 3 && !resumed; ++attempt) {
    auto completed = alyrn::uring::detail::LoopAccess::WaitCompletions(loop);
    if (!completed.HasValue()) {
      break;
    }
    completion_received = true;
    alyrn::uring::detail::LoopAccess::RunReady(loop);
  }

  const bool passed = Check(completion_received, "sleep should complete") &&
                      Check(resumed, "SleepFor should resume the coroutine") &&
                      Check(scheduler_ok, "SleepFor should resume on its loop scheduler");
  return StopAndDrain(loop) && passed;
}

bool TestStopDiscardsUnexpiredTimer() {
  bool fired = false;
  {
    alyrn::uring::Loop loop;
    alyrn::uring::Options options;
    options.entries = 8;

    auto init = loop.Init(options);
    if (!init.HasValue()) {
      if (IsEnvironmentSkip(init.Error())) {
        return true;
      }
      return Check(false, "Loop initialization failed for stop test");
    }

    auto timer = loop.RunAfter(1h, [&fired] noexcept { fired = true; });
    if (!Check(timer.HasValue(), "unexpired timer should be accepted")) {
      return false;
    }

    std::jthread stopper([&loop] {
      std::this_thread::sleep_for(2ms);
      loop.RequestStop();
    });
    loop.Run();
    stopper.join();

    if (!Check(loop.State() == alyrn::io::LoopState::kStopped,
               "loop with an unexpired timer should stop")) {
      return false;
    }
  }

  return Check(!fired, "loop shutdown must discard an unexpired timer without running it");
}


}  // namespace

int main() {
  if (!TestLoopAffinityIsEnforcedInRelease()) return 1;
  if (!TestTimers()) return 1;
  if (!TestStopDiscardsUnexpiredTimer()) return 1;
  std::cout << "luring timer smoke: PASS\n";
  return 0;
}
