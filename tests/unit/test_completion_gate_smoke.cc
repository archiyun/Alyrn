// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <csignal>
#include <cstdio>

#include "coropact/coro/scheduler.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/utils/macros.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (condition) {
    return true;
  }
  std::fprintf(stderr, "FAIL: %s\n", message);
  return false;
}

bool ExpectChildAbort(void (*entry)(), const char* message) {
  const pid_t child = ::fork();
  if (child < 0) {
    return Expect(false, "fork failed for continuation invariant test");
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    entry();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Expect(WIFSIGNALED(status), message) &&
         Expect(WTERMSIG(status) == SIGABRT, "continuation invariant must terminate with SIGABRT");
}

template <typename T>
concept ResettableCompletionGate = requires(T& gate) { gate.Reset(); };

template <typename T>
concept ReassignableCompletionGate = requires(T& gate) { gate = {}; };

static_assert(!ResettableCompletionGate<coropact::operation::detail::CompletionGate>);
static_assert(!ReassignableCompletionGate<coropact::operation::detail::CompletionGate>);

bool TestOneShotTransition() {
  coropact::operation::detail::CompletionGate gate;

  bool ok = true;
  ok &= Expect(!gate.Completed(), "a new completion gate must be open");
  ok &= Expect(gate.TryComplete(), "the first completion must win");
  ok &= Expect(gate.Completed(), "a winning completion must become terminal");
  ok &= Expect(!gate.TryComplete(), "a duplicate completion must be rejected");
  return ok;
}

class RecordingScheduler final : public coropact::coro::Scheduler {
public:
  void Schedule(coropact::coro::Work* work) noexcept override { scheduled_ = work; }

  coropact::coro::Work* scheduled_{nullptr};
};

void ScheduleUnboundContinuation() {
  coropact::operation::detail::SchedulerContinuation continuation;
  continuation.Schedule();
}

void BindContinuationTwice() {
  RecordingScheduler scheduler;
  coropact::coro::Scheduler::SetCurrent(&scheduler);

  coropact::operation::detail::SchedulerContinuation continuation;
  continuation.Bind(std::noop_coroutine());
  continuation.Bind(std::noop_coroutine());
}

void BindContinuationWithoutScheduler() {
  coropact::coro::Scheduler::SetCurrent(nullptr);
  coropact::operation::detail::SchedulerContinuation continuation;
  continuation.Bind(std::noop_coroutine());
}

bool TestSchedulerContinuationPreservesAffinity() {
  RecordingScheduler scheduler;
  auto* const previous = coropact::coro::Scheduler::TryCurrent();
  coropact::coro::Scheduler::SetCurrent(&scheduler);

  coropact::operation::detail::SchedulerContinuation continuation;
  continuation.Bind(std::noop_coroutine());
  coropact::coro::Scheduler::SetCurrent(previous);

  continuation.Schedule();
  return Expect(continuation.Bound(), "a bound continuation must retain its scheduler") &&
         Expect(scheduler.scheduled_ != nullptr,
                "completion must schedule through the captured scheduler");
}

bool TestSchedulerContinuationRejectsInvalidTransitions() {
  return ExpectChildAbort(&ScheduleUnboundContinuation,
                          "unbound continuation scheduling must terminate in Release") &&
         ExpectChildAbort(&BindContinuationTwice,
                          "duplicate continuation binding must terminate in Release") &&
         ExpectChildAbort(&BindContinuationWithoutScheduler,
                          "binding without an owner scheduler must terminate in Release");
}

}  // namespace

int main() {
  const bool ok = TestOneShotTransition() && TestSchedulerContinuationPreservesAffinity() &&
                  TestSchedulerContinuationRejectsInvalidTransitions();
  if (ok) {
    std::puts("completion gate smoke: PASS");
    return 0;
  }
  return 1;
}
