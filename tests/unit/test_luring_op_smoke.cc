// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <csignal>
#include <cstdio>

#include "coropact/luring/detail/cancel_result.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/detail/result_state.h"
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
    return Expect(false, "fork failed for luring result invariant test");
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
         Expect(WTERMSIG(status) == SIGABRT, "luring result invariant must terminate with SIGABRT");
}

void TakePendingResultState() {
  coropact::luring::detail::LUringResultState<void> state;
  (void)state.Take();
}

void SetResultStateTwice() {
  coropact::luring::detail::LUringResultState<void> state;
  state.SetSuccess();
  state.SetError(coropact::base::MakeErrno(EPIPE));
}

void ReadEmptyCqeResult() {
  coropact::luring::detail::LUringCqeResult result;
  (void)*result;
}

void SetCqeResultTwice() {
  coropact::luring::detail::LUringCqeResult result;
  result = 1;
  result = 2;
}

bool TestSingleResultCompletion() {
  coropact::luring::detail::LUringOp op;
  op.kind = coropact::luring::detail::LUringOpKind::kReadComplete;

  bool ok = true;
  ok &= Expect(op.Complete(17), "the first CQE must complete the operation");
  ok &= Expect(op.IsCompleted(), "the operation must become terminal after its first CQE");
  ok &= Expect(*op.result == 17, "the winning CQE result must be retained");
  ok &= Expect(!op.Complete(-5), "a duplicate CQE must not overwrite the result");
  ok &= Expect(*op.result == 17, "a duplicate CQE must preserve the original result");
  ok &= Expect(op.DispatchKind() == coropact::luring::detail::LUringOpKind::kReadComplete,
               "completion state must not alter dispatch kind");
  return ok;
}

bool TestReusablePhysicalSlot() {
  coropact::luring::detail::LUringOp op;
  op.kind = coropact::luring::detail::LUringOpKind::kWake;
  (void)(op.Complete(0));
  op.resume_work.SetHandle(std::noop_coroutine());
  op.BeginNextRequest();

  return Expect(!op.IsCompleted(), "reset must reopen a reusable operation slot") &&
         Expect(!op.result.HasValue(), "next request must not retain a prior CQE result") &&
         Expect(!op.resume_work.HasHandle(), "next request must not retain a prior continuation") &&
         Expect(op.Complete(0), "a reopened operation slot must accept a CQE") &&
         Expect(op.DispatchKind() == coropact::luring::detail::LUringOpKind::kWake,
                "reset must preserve dispatch kind");
}

bool TestCompletionModels() {
  using coropact::luring::detail::CompletionModelFor;
  using coropact::luring::detail::LUringCompletionModel;
  using coropact::luring::detail::LUringOpKind;

  bool ok = true;
  ok &=
      Expect(CompletionModelFor(LUringOpKind::kReadComplete) == LUringCompletionModel::kSingleShot,
             "ordinary I/O must use the single-shot completion model");
  ok &= Expect(CompletionModelFor(LUringOpKind::kAcceptSourceComplete) ==
                   LUringCompletionModel::kEventStream,
               "accept sources must use the event-stream completion model");
  ok &= Expect(
      CompletionModelFor(LUringOpKind::kRecvSourceComplete) == LUringCompletionModel::kEventStream,
      "recv sources must use the event-stream completion model");
  ok &= Expect(CompletionModelFor(LUringOpKind::kSendZeroCopyComplete) ==
                   LUringCompletionModel::kSplitRelease,
               "zero-copy send must use the split-release completion model");
  return ok;
}

bool TestCancelCqeClassification() {
  using coropact::luring::detail::IsExpectedCancelCqeResult;

  bool ok = true;
  ok &= Expect(IsExpectedCancelCqeResult(0), "successful cancellation must be expected");
  ok &= Expect(IsExpectedCancelCqeResult(2), "cancel-all count must be expected");
  ok &= Expect(IsExpectedCancelCqeResult(-ENOENT), "target completion race must be expected");
  ok &= Expect(IsExpectedCancelCqeResult(-EALREADY),
               "target-in-progress cancellation race must be expected");
  ok &= Expect(IsExpectedCancelCqeResult(-ECANCELED),
               "cancel command stopped by loop shutdown must be expected");
  ok &= Expect(!IsExpectedCancelCqeResult(-EINVAL),
               "invalid cancel SQE must remain a source terminal error");
  return ok;
}

bool TestResultStatesRejectInvalidTransitions() {
  return ExpectChildAbort(&TakePendingResultState,
                          "pending luring result Take must terminate in Release") &&
         ExpectChildAbort(&SetResultStateTwice,
                          "duplicate luring result completion must terminate in Release") &&
         ExpectChildAbort(&ReadEmptyCqeResult,
                          "empty CQE result access must terminate in Release") &&
         ExpectChildAbort(&SetCqeResultTwice,
                          "duplicate CQE result assignment must terminate in Release");
}

}  // namespace

int main() {
  const bool ok = TestSingleResultCompletion() && TestReusablePhysicalSlot() &&
                  TestCompletionModels() && TestCancelCqeClassification() &&
                  TestResultStatesRejectInvalidTransitions();
  if (ok) {
    std::puts("luring op smoke: PASS");
    return 0;
  }
  return 1;
}
