// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <cstdio>
#include <coroutine>

#include "coropact/coro/scheduler.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (condition) {
    return true;
  }
  std::fprintf(stderr, "FAIL: %s\n", message);
  return false;
}

bool TestOneShotTransition() {
  coropact::operation::detail::CompletionGate gate;

  bool ok = true;
  ok &= Expect(!gate.Completed(), "a new completion gate must be open");
  ok &= Expect(gate.TryComplete(), "the first completion must win");
  ok &= Expect(gate.Completed(), "a winning completion must become terminal");
  ok &= Expect(!gate.TryComplete(), "a duplicate completion must be rejected");
  return ok;
}

bool TestResetForReusablePhysicalSlot() {
  coropact::operation::detail::CompletionGate gate;
  static_cast<void>(gate.TryComplete());
  gate.Reset();

  return Expect(!gate.Completed(), "reset must reopen a reusable physical slot") &&
         Expect(gate.TryComplete(), "a reopened slot must accept its next completion");
}

class RecordingScheduler final : public coropact::coro::Scheduler {
 public:
  void Schedule(coropact::coro::Work* work) noexcept override { scheduled_ = work; }

  coropact::coro::Work* scheduled_{nullptr};
};

bool TestSchedulerContinuationPreservesAffinity() {
  RecordingScheduler scheduler;
  auto* const previous = coropact::coro::Scheduler::Current();
  coropact::coro::Scheduler::SetCurrent(&scheduler);

  coropact::operation::detail::SchedulerContinuation continuation;
  continuation.Bind(std::noop_coroutine());
  coropact::coro::Scheduler::SetCurrent(previous);

  continuation.Schedule();
  return Expect(continuation.Bound(), "a bound continuation must retain its scheduler") &&
         Expect(scheduler.scheduled_ != nullptr,
                "completion must schedule through the captured scheduler");
}

}  // namespace

int main() {
  const bool ok = TestOneShotTransition() && TestResetForReusablePhysicalSlot() &&
                  TestSchedulerContinuationPreservesAffinity();
  if (ok) {
    std::puts("completion gate smoke: PASS");
    return 0;
  }
  return 1;
}
