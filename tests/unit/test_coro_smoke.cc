// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Smoke test for the vexo::coro module (M0):
//   a) Task<int> awaited by another Task<int>, result via SyncWait;
//   b) Task<void> path;
//   c) Spawn + JoinHandle on a drain-queue Scheduler: Wait, Detach, async join;
//   d) error path: Task<Result<int>> co_return std::unexpected(...).
// The module is IO-agnostic: the only scheduler here is a test-local container.

#include <cassert>
#include <cerrno>
#include <expected>
#include <iostream>
#include <memory>
#include <system_error>

#include "vexo/base/error.h"
#include "vexo/coro/awaitable.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/spawn.h"
#include "vexo/coro/sync_wait.h"
#include "vexo/coro/task.h"
#include "vexo/coro/work.h"

using vexo::base::make_errno;
using vexo::base::Result;
using vexo::coro::JoinHandle;
using vexo::coro::Scheduler;
using vexo::coro::Spawn;
using vexo::coro::SyncWait;
using vexo::coro::Task;
using vexo::coro::Work;
using vexo::coro::WorkQueue;

namespace {

bool Check(bool condition, const char* message) {
  if (condition) return true;
  std::cout << "test failed: " << message << '\n';
  return false;
}

struct RawAwaiter {
  bool await_ready() const noexcept { return true; }
  void await_suspend(std::coroutine_handle<>) const noexcept {}
  int await_resume() const noexcept { return 7; }
};

struct MemberAwaitable {
  RawAwaiter operator co_await() && noexcept { return {}; }
};

struct AdlAwaitable {};
RawAwaiter operator co_await(AdlAwaitable&&) noexcept { return {}; }

struct BothAwaitable {
  RawAwaiter operator co_await() && noexcept { return {}; }
};
RawAwaiter operator co_await(BothAwaitable&&) noexcept { return {}; }

struct PromiseMarker {};
struct PromiseAwareAwaiter {
  bool await_ready() const noexcept { return true; }
  void await_suspend(std::coroutine_handle<PromiseMarker>) const noexcept {}
  void await_resume() const noexcept {}
};

struct TransformPromise {
  RawAwaiter await_transform(int) noexcept { return {}; }
};

struct BadAwaiter {
  bool await_ready() const noexcept { return true; }
  int await_suspend(std::coroutine_handle<>) const noexcept { return 0; }
  void await_resume() const noexcept {}
};

// The leaf Task and its composition. Task<int> is the awaitable that satisfies
// the Awaitable concept used to constrain Spawn/SyncWait inputs.
static_assert(vexo::coro::Awaitable<Task<int>>);
static_assert(vexo::coro::Awaitable<Task<void>>);
static_assert(vexo::coro::Awaitable<RawAwaiter>);
static_assert(vexo::coro::Awaitable<MemberAwaitable>);
static_assert(vexo::coro::Awaitable<AdlAwaitable>);
static_assert(vexo::coro::Awaitable<BothAwaitable>);
static_assert(std::same_as<vexo::coro::AwaitResult<AdlAwaitable>, int>);
static_assert(vexo::coro::AwaiterFor<PromiseAwareAwaiter, PromiseMarker>);
static_assert(vexo::coro::PromiseTransformedAwaitable<TransformPromise, int>);
static_assert(!vexo::coro::Awaiter<PromiseAwareAwaiter>);
static_assert(!vexo::coro::Awaiter<BadAwaiter>);

Task<int> Add(int a, int b) { co_return a + b; }

Task<int> Sum() {
  int x = co_await Add(20, 22);  // symmetric transfer into the child
  int y = co_await Add(1, 0);
  co_return x + y;
}

Task<std::unique_ptr<int>> MoveOnly() { co_return std::make_unique<int>(99); }

Task<Result<int>> Fail() { co_return std::unexpected(make_errno(EINVAL)); }

int g_void_marker = 0;
Task<void> SetMarker() {
  g_void_marker = 7;
  co_return;
}

// Drain-queue Scheduler: collects Work* and runs them FIFO. Running a Work may
// enqueue more (e.g. a parked joiner being resumed), so the loop re-checks.
class DrainScheduler final : public Scheduler {
public:
  void Schedule(Work* work) noexcept override {
    const bool queued = queue_.PushBack(work);
    assert(queued);
    (void)queued;
  }

  void Drain() {
    while (Work* work = queue_.PopFront()) {
      work->Run();
    }
  }

private:
  WorkQueue queue_;
};

// Parent coroutine that joins two spawned children asynchronously.
Task<int> JoinChildren(DrainScheduler* sched) {
  int a = co_await Spawn(*sched, Add(100, 0));
  int b = co_await Spawn(*sched, Add(0, 11));
  co_return a + b;
}

}  // namespace

int main() {
  // a) Task awaiting Task, value through SyncWait.
  if (!Check(SyncWait(Sum()) == 43, "Sum should be 43")) return 1;
  if (!Check(SyncWait(Add(2, 3)) == 5, "Add should be 5")) return 1;

  // move-only result survives the frame.
  {
    std::unique_ptr<int> p = SyncWait(MoveOnly());
    if (!Check(p && *p == 99, "move-only result preserved")) return 1;
  }

  // b) Task<void> path.
  g_void_marker = 0;
  SyncWait(SetMarker());
  if (!Check(g_void_marker == 7, "Task<void> should run")) return 1;

  // c) Spawn + JoinHandle on a drain-queue scheduler.
  {
    DrainScheduler sched;

    // Wait path.
    JoinHandle<int> joinable = Spawn(sched, Add(40, 2));
    sched.Drain();
    if (!Check(joinable.Wait() == 42, "spawn+Wait should yield 42")) return 1;

    // Detach path: result discarded, coroutine still runs to completion.
    g_void_marker = 0;
    JoinHandle<void> detached = Spawn(sched, SetMarker());
    detached.Detach();
    sched.Drain();
    if (!Check(g_void_marker == 7, "spawn+Detach should still run the body")) return 1;

    // Async join: a spawned parent co_awaits two spawned children.
    JoinHandle<int> parent = Spawn(sched, JoinChildren(&sched));
    sched.Drain();
    if (!Check(parent.Wait() == 111, "async join should yield 111")) return 1;
  }

  // d) Error path travels through the value channel, no exceptions.
  {
    Result<int> r = SyncWait(Fail());
    if (!Check(!r.has_value(), "Fail should not hold a value")) return 1;
    if (!Check(r.error() == std::errc::invalid_argument, "Fail should carry EINVAL")) return 1;
  }

  std::cout << "coro smoke: PASS\n";
  return 0;
}
