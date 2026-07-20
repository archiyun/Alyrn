// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <chrono>
#include <iostream>

#include "vexo/coro/spawn.h"
#include "vexo/coro/task.h"
#include "vexo/luring/connector.h"
#include "vexo/luring/loop.h"
#include "vexo/luring/options.h"
#include "vexo/luring/timer.h"

namespace {

using namespace std::chrono_literals;

static_assert(requires(vexo::luring::LUringConnector& connector) { connector.SleepFor(1ms); });

bool Check(bool condition, const char* message) {
  if (!condition) std::cout << "FAIL: " << message << '\n';
  return condition;
}

bool IsEnvironmentSkip(vexo::base::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

vexo::coro::Task<void> SleepTask(vexo::luring::LUringLoop* loop, bool* resumed,
                                 bool* scheduler_ok) {
  auto result = co_await vexo::luring::SleepFor(*loop, 1ms);
  *resumed = true;
  *scheduler_ok = vexo::coro::Scheduler::Current() == loop;
  if (!result.has_value()) co_return;
}

bool TestTimers() {
  vexo::luring::LUringLoop loop;
  vexo::luring::LUringOptions options;
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
  if (!Check(late.has_value(), "late timer should be accepted")) return false;

  auto early = loop.RunAfter(2ms, [&early_fired] noexcept { early_fired = true; });
  if (!Check(early.has_value(), "early timer should be accepted")) return false;

  auto completed = loop.WaitCompletions();
  if (!Check(completed.has_value(), "timer completion should be received") ||
      !Check(early_fired, "earlier timer should fire first") ||
      !Check(!late_fired, "later timer should not fire early")) {
    return false;
  }

  if (!Check(loop.CancelTimer(*late).has_value(), "later timer should be cancellable")) {
    return false;
  }

  bool resumed = false;
  bool scheduler_ok = false;
  vexo::coro::Spawn(loop, SleepTask(&loop, &resumed, &scheduler_ok)).Detach();
  loop.RunReady();
  completed = loop.WaitCompletions();
  loop.RunReady();

  return Check(completed.has_value(), "sleep should complete") &&
         Check(resumed, "SleepFor should resume the coroutine") &&
         Check(scheduler_ok, "SleepFor should resume on its loop scheduler");
}

}  // namespace

int main() {
  if (!TestTimers()) return 1;
  std::cout << "luring timer smoke: PASS\n";
  return 0;
}
