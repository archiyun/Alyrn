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

class BindContinuationWork final : public coropact::coro::Work {
public:
  BindContinuationWork(coropact::operation::detail::SchedulerContinuation* continuation,
                       bool bind_twice) noexcept
      : continuation_(continuation), bind_twice_(bind_twice) {
    SetRun(&RunBind);
  }

private:
  static void RunBind(coropact::coro::Work* work) noexcept {
    auto* self = static_cast<BindContinuationWork*>(work);
    self->continuation_->Bind(std::noop_coroutine());
    if (self->bind_twice_) {
      self->continuation_->Bind(std::noop_coroutine());
    }
  }

  coropact::operation::detail::SchedulerContinuation* continuation_;
  bool bind_twice_;
};

class ObserveCurrentWork final : public coropact::coro::Work {
public:
  explicit ObserveCurrentWork(coropact::coro::Scheduler** observed) noexcept : observed_(observed) {
    SetRun(&RunObserve);
  }

private:
  static void RunObserve(coropact::coro::Work* work) noexcept {
    auto* self = static_cast<ObserveCurrentWork*>(work);
    *self->observed_ = coropact::coro::Scheduler::TryCurrent();
  }

  coropact::coro::Scheduler** observed_;
};

class RunNestedWork final : public coropact::coro::Work {
public:
  RunNestedWork(coropact::coro::Scheduler* nested_scheduler, coropact::coro::Work* nested_work,
                coropact::coro::Scheduler** before, coropact::coro::Scheduler** after) noexcept
      : nested_scheduler_(nested_scheduler),
        nested_work_(nested_work),
        before_(before),
        after_(after) {
    SetRun(&RunNested);
  }

private:
  static void RunNested(coropact::coro::Work* work) noexcept {
    auto* self = static_cast<RunNestedWork*>(work);
    *self->before_ = coropact::coro::Scheduler::TryCurrent();
    self->nested_scheduler_->Run(self->nested_work_);
    *self->after_ = coropact::coro::Scheduler::TryCurrent();
  }

  coropact::coro::Scheduler* nested_scheduler_;
  coropact::coro::Work* nested_work_;
  coropact::coro::Scheduler** before_;
  coropact::coro::Scheduler** after_;
};

void ScheduleUnboundContinuation() {
  coropact::operation::detail::SchedulerContinuation continuation;
  continuation.Schedule();
}

void BindContinuationTwice() {
  RecordingScheduler scheduler;
  coropact::operation::detail::SchedulerContinuation continuation;
  BindContinuationWork work{&continuation, true};
  scheduler.Run(&work);
}

void BindContinuationWithoutScheduler() {
  coropact::operation::detail::SchedulerContinuation continuation;
  continuation.Bind(std::noop_coroutine());
}

bool TestSchedulerContinuationPreservesAffinity() {
  RecordingScheduler scheduler;
  coropact::operation::detail::SchedulerContinuation continuation;
  BindContinuationWork work{&continuation, false};
  scheduler.Run(&work);

  continuation.Schedule();
  return Expect(continuation.Bound(), "a bound continuation must retain its scheduler") &&
         Expect(scheduler.scheduled_ != nullptr,
                "completion must schedule through the captured scheduler");
}

bool TestSchedulerContinuationUsesCustomDispatch() {
  RecordingScheduler scheduler;
  coropact::operation::detail::SchedulerContinuation continuation;
  BindContinuationWork work{&continuation, false};
  scheduler.Run(&work);

  bool dispatched = false;
  coropact::coro::Scheduler* observed_scheduler = nullptr;
  coropact::coro::Work* observed_work = nullptr;
  continuation.ScheduleWith(
      [&](coropact::coro::Scheduler& owner, coropact::coro::Work* resume_work) noexcept {
        dispatched = true;
        observed_scheduler = &owner;
        observed_work = resume_work;
      });

  return Expect(dispatched, "custom continuation dispatch must run") &&
         Expect(observed_scheduler == &scheduler,
                "custom continuation dispatch must receive the captured scheduler") &&
         Expect(observed_work != nullptr,
                "custom continuation dispatch must receive the resume work") &&
         Expect(scheduler.scheduled_ == nullptr,
                "custom continuation dispatch must not also use default Schedule");
}

bool TestSchedulerRunRestoresNestedAffinity() {
  RecordingScheduler outer_scheduler;
  RecordingScheduler inner_scheduler;
  coropact::coro::Scheduler* outer_before = nullptr;
  coropact::coro::Scheduler* inner_observed = nullptr;
  coropact::coro::Scheduler* outer_after = nullptr;

  ObserveCurrentWork inner_work{&inner_observed};
  RunNestedWork outer_work{&inner_scheduler, &inner_work, &outer_before, &outer_after};
  outer_scheduler.Run(&outer_work);

  return Expect(outer_before == &outer_scheduler,
                "outer Scheduler::Run must publish the outer scheduler") &&
         Expect(inner_observed == &inner_scheduler,
                "nested Scheduler::Run must publish the nested scheduler") &&
         Expect(outer_after == &outer_scheduler,
                "nested Scheduler::Run must restore the outer scheduler") &&
         Expect(coropact::coro::Scheduler::TryCurrent() == nullptr,
                "outer Scheduler::Run must restore the prior scheduler");
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
                  TestSchedulerContinuationUsesCustomDispatch() &&
                  TestSchedulerRunRestoresNestedAffinity() &&
                  TestSchedulerContinuationRejectsInvalidTransitions();
  if (ok) {
    std::puts("completion gate smoke: PASS");
    return 0;
  }
  return 1;
}
