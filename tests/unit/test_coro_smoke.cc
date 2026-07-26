// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Smoke test for the coropact::coro module (M0):
//   a) Task<int> awaited by another Task<int>, result via SyncWait;
//   b) Task<void> path;
//   c) Spawn + JoinHandle on a drain-queue Scheduler: Wait, Detach, async join;
//   d) SpawnDetach: discarded result and suspended/resumed fire-and-forget work;
//   e) error path: Task<Result<int>> co_return std::unexpected(...).
// The module is IO-agnostic: the only scheduler here is a test-local container.

#include <cassert>
#include <cerrno>
#include <coroutine>
#include <expected>
#include <iostream>
#include <memory>
#include <system_error>

#include "coropact/base/error.h"
#include "coropact/coro/awaitable.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/sync_wait.h"
#include "coropact/coro/task.h"
#include "coropact/coro/work.h"

using coropact::base::make_errno;
using coropact::base::Result;
using coropact::coro::JoinHandle;
using coropact::coro::Scheduler;
using coropact::coro::Spawn;
using coropact::coro::SpawnDetach;
using coropact::coro::SyncWait;
using coropact::coro::Task;
using coropact::coro::Work;
using coropact::coro::WorkQueue;

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
static_assert(coropact::coro::Awaitable<Task<int>>);
static_assert(coropact::coro::Awaitable<Task<void>>);
static_assert(coropact::coro::Awaitable<RawAwaiter>);
static_assert(coropact::coro::Awaitable<MemberAwaitable>);
static_assert(coropact::coro::Awaitable<AdlAwaitable>);
static_assert(coropact::coro::Awaitable<BothAwaitable>);
static_assert(std::same_as<coropact::coro::AwaitResult<AdlAwaitable>, int>);
static_assert(coropact::coro::AwaiterFor<PromiseAwareAwaiter, PromiseMarker>);
static_assert(coropact::coro::PromiseTransformedAwaitable<TransformPromise, int>);
static_assert(!coropact::coro::Awaiter<PromiseAwareAwaiter>);
static_assert(!coropact::coro::Awaiter<BadAwaiter>);

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

Task<int> DetachedReturn(bool* ran, Scheduler** resumed_scheduler) {
  *ran = true;
  *resumed_scheduler = Scheduler::Current();
  co_return 99;
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
      Run(work);
    }
  }

  bool DrainOne() {
    Work* work = queue_.PopFront();
    if (work == nullptr) {
      return false;
    }
    Run(work);
    return true;
  }

private:
  WorkQueue queue_;
};

// A one-shot awaitable used to prove that SpawnDetach keeps its driver frame
// alive while the child Task is suspended. The ResumeWork is owned by this
// test gate and is submitted only when Open() is called.
struct ManualGate {
  struct Awaiter {
    ManualGate* gate;

    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> waiter) noexcept {
      gate->resume_work.SetHandle(waiter);
      return true;
    }

    void await_resume() const noexcept {}
  };

  Awaiter Wait() noexcept { return Awaiter{this}; }

  void Open(DrainScheduler& scheduler) noexcept {
    assert(resume_work.HasHandle());
    scheduler.Schedule(&resume_work);
  }

  coropact::coro::ResumeWork resume_work;
};

Task<void> WaitForGate(ManualGate* gate, bool* resumed, Scheduler** resumed_scheduler) {
  co_await gate->Wait();
  *resumed = true;
  *resumed_scheduler = Scheduler::Current();
}

// Parent coroutine that joins two spawned children asynchronously.
Task<int> JoinChildren(DrainScheduler* sched) {
  int a = co_await Spawn(*sched, Add(100, 0));
  int b = co_await Spawn(*sched, Add(0, 11));
  co_return a + b;
}

Task<void> JoinChildAndMark(DrainScheduler* sched, bool* parent_resumed) {
  auto child = Spawn(*sched, Add(40, 2));
  (void)co_await std::move(child);
  *parent_resumed = true;
}

Task<void> JoinChildFromOtherScheduler(DrainScheduler* child_sched, bool* parent_resumed,
                                       Scheduler** resumed_scheduler) {
  auto child = Spawn(*child_sched, Add(40, 2));
  (void)co_await std::move(child);
  *parent_resumed = true;
  *resumed_scheduler = Scheduler::Current();
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

    // Direct SpawnDetach path: a non-void result is discarded, while the body
    // still runs under the scheduler that accepted the work.
    bool direct_detached_ran = false;
    Scheduler* direct_detached_scheduler = nullptr;
    SpawnDetach(sched, DetachedReturn(&direct_detached_ran, &direct_detached_scheduler));
    if (!Check(!direct_detached_ran, "SpawnDetach should be lazy until scheduled")) return 1;
    if (!Check(sched.DrainOne(), "SpawnDetach should enqueue a driver Work")) return 1;
    if (!Check(direct_detached_ran, "SpawnDetach should run the body")) return 1;
    if (!Check(direct_detached_scheduler == &sched,
               "SpawnDetach should run under the current scheduler")) {
      return 1;
    }

    // Suspended SpawnDetach path: the driver and its Work must remain alive
    // until the external completion schedules the child back to this loop.
    ManualGate gate;
    bool gate_resumed = false;
    Scheduler* gate_scheduler = nullptr;
    SpawnDetach(sched, WaitForGate(&gate, &gate_resumed, &gate_scheduler));
    if (!Check(sched.DrainOne(), "SpawnDetach should start the suspended task")) return 1;
    if (!Check(!gate_resumed, "suspended SpawnDetach must not finish early")) return 1;
    gate.Open(sched);
    if (!Check(sched.DrainOne(), "opened gate should resume SpawnDetach")) return 1;
    if (!Check(gate_resumed, "resumed SpawnDetach should finish")) return 1;
    if (!Check(gate_scheduler == &sched,
               "resumed SpawnDetach should use the current scheduler")) {
      return 1;
    }

    // Async join resumes through a scheduled ResumeWork rather than directly
    // from SpawnState::Finish(). The parent must not resume inline while the
    // child Work is still running.
    bool parent_resumed = false;
    JoinHandle<void> deferred_parent = Spawn(sched, JoinChildAndMark(&sched, &parent_resumed));
    if (!Check(sched.DrainOne(), "deferred join should have a parent root Work")) return 1;
    if (!Check(sched.DrainOne(), "deferred join should have a child root Work")) return 1;
    if (!Check(!parent_resumed, "async join parent must not resume inline")) return 1;
    sched.Drain();
    if (!Check(parent_resumed, "async join parent should resume from scheduled Work")) return 1;
    deferred_parent.Wait();

    // The joiner must return to the parent's Scheduler even when the child is
    // started on another Scheduler.
    DrainScheduler child_sched;
    bool cross_parent_resumed = false;
    Scheduler* resumed_scheduler = nullptr;
    JoinHandle<void> cross_parent =
        Spawn(sched, JoinChildFromOtherScheduler(&child_sched, &cross_parent_resumed,
                                                 &resumed_scheduler));
    if (!Check(sched.DrainOne(), "cross-scheduler join should start the parent")) return 1;
    if (!Check(child_sched.DrainOne(), "cross-scheduler join should start the child")) return 1;
    if (!Check(!cross_parent_resumed, "cross-scheduler parent must remain parked")) return 1;
    if (!Check(sched.DrainOne(), "cross-scheduler join should schedule the parent on its loop")) {
      return 1;
    }
    if (!Check(cross_parent_resumed, "cross-scheduler parent should resume")) return 1;
    if (!Check(resumed_scheduler == &sched,
               "cross-scheduler parent should resume on the parent scheduler")) {
      return 1;
    }
    cross_parent.Wait();

    // Async join: a spawned parent co_awaits two spawned children.
    JoinHandle<int> parent = Spawn(sched, JoinChildren(&sched));
    sched.Drain();
    if (!Check(parent.Wait() == 111, "async join should yield 111")) return 1;
  }

  // e) Error path travels through the value channel, no exceptions.
  {
    Result<int> r = SyncWait(Fail());
    if (!Check(!r.has_value(), "Fail should not hold a value")) return 1;
    if (!Check(r.error() == std::errc::invalid_argument, "Fail should carry EINVAL")) return 1;
  }

  std::cout << "coro smoke: PASS\n";
  return 0;
}
