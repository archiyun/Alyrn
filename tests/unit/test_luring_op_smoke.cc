// SPDX-License-Identifier: MIT

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <csignal>
#include <cstdio>

#include "alyrn/detail/uring/cancel_result.h"
#include "alyrn/detail/uring/op.h"
#include "alyrn/detail/uring/result_state.h"
#include "alyrn/detail/macros.h"

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
  alyrn::uring::detail::ResultState<void> state;
  (void)state.Take();
}

void SetResultStateTwice() {
  alyrn::uring::detail::ResultState<void> state;
  state.SetSuccess();
  state.SetError(alyrn::Errno(EPIPE));
}

void ReadEmptyCqeResult() {
  alyrn::uring::detail::CqeResult result;
  (void)*result;
}

void SetCqeResultTwice() {
  alyrn::uring::detail::CqeResult result;
  result = 1;
  result = 2;
}

bool TestSingleResultCompletion() {
  alyrn::uring::detail::Op op;
  op.kind = alyrn::uring::detail::OpKind::kReadComplete;

  bool ok = true;
  ok &= Expect(op.TryRecordCqeCompletion(17), "the first CQE must complete the operation");
  ok &=
      Expect(op.CqeCompletionRecorded(), "the operation must become terminal after its first CQE");
  ok &= Expect(*op.result == 17, "the winning CQE result must be retained");
  ok &= Expect(op.CoupledResultReady(), "the CQE result must enter the coupled result-ready phase");
  ok &= Expect(!op.CoupledReleaseAuthorized(),
               "result readiness must not implicitly authorize resource release");
  ok &= Expect(op.TryAuthorizeCoupledRelease(),
               "coupled release must be authorized after the CQE result is ready");
  ok &= Expect(op.CoupledReleaseAuthorized(), "coupled release authorization must be observable");
  ok &= Expect(op.TryAuthorizeCoupledContinuation(),
               "continuation must be authorized after coupled release");
  ok &= Expect(op.CoupledContinuationAuthorized(),
               "coupled continuation authorization must be observable");
  ok &= Expect(!op.TryRecordCqeCompletion(-5), "a duplicate CQE must not overwrite the result");
  ok &= Expect(*op.result == 17, "a duplicate CQE must preserve the original result");
  ok &= Expect(op.DispatchKind() == alyrn::uring::detail::OpKind::kReadComplete,
               "completion state must not alter dispatch kind");
  return ok;
}

bool TestReusablePhysicalSlot() {
  alyrn::uring::detail::Op op;
  op.kind = alyrn::uring::detail::OpKind::kWake;
  (void)(op.TryRecordCqeCompletion(0));
  op.resume_work.SetHandle(std::noop_coroutine());
  op.BeginNextRequest();

  return Expect(!op.CqeCompletionRecorded(), "reset must reopen a reusable operation slot") &&
         Expect(!op.result.HasValue(), "next request must not retain a prior CQE result") &&
         Expect(!op.resume_work.HasHandle(), "next request must not retain a prior continuation") &&
         Expect(op.TryRecordCqeCompletion(0), "a reopened operation slot must accept a CQE") &&
         Expect(op.DispatchKind() == alyrn::uring::detail::OpKind::kWake,
                "reset must preserve dispatch kind");
}

bool TestReusableCoupledLifecycle() {
  alyrn::uring::detail::Op op;
  op.kind = alyrn::uring::detail::OpKind::kReadComplete;
  if (!Expect(op.TryRecordCqeCompletion(3), "initial coupled request must accept its CQE") ||
      !Expect(op.TryAuthorizeCoupledRelease(), "initial coupled request must authorize release") ||
      !Expect(op.TryAuthorizeCoupledContinuation(),
              "initial coupled request must authorize continuation")) {
    return false;
  }

  op.BeginNextRequest();
  return Expect(!op.CqeCompletionRecorded(),
                "next coupled request must not retain a CQE completion") &&
         Expect(!op.result.HasValue(), "next coupled request must not retain a CQE result") &&
         Expect(!op.CoupledResultReady(), "next coupled request must reset result readiness") &&
         Expect(!op.CoupledReleaseAuthorized(),
                "next coupled request must reset release authorization") &&
         Expect(!op.CoupledContinuationAuthorized(),
                "next coupled request must reset continuation authorization") &&
         Expect(op.TryRecordCqeCompletion(7), "reset coupled request must accept a new CQE") &&
         Expect(*op.result == 7, "reset coupled request must retain its new CQE result");
}

bool TestConnectCqeRequiresAdapterRefinement() {
  alyrn::uring::detail::Op op;
  op.kind = alyrn::uring::detail::OpKind::kConnect;

  return Expect(op.TryRecordCqeCompletion(0), "Connect CQE must settle its physical request") &&
         Expect(op.CqeCompletionRecorded(), "Connect CQE must settle its physical slot") &&
         Expect(!op.CoupledResultReady(),
                "a raw Connect CQE must not bypass logical stream construction") &&
         Expect(op.TryAuthorizeCoupledResult(),
                "the Connect adapter must authorize its constructed logical result") &&
         Expect(op.TryAuthorizeCoupledRelease(),
                "Connect release must follow logical stream construction") &&
         Expect(op.TryAuthorizeCoupledContinuation(),
                "Connect continuation must follow resource release");
}

bool TestAcceptCqeRequiresAdapterRefinement() {
  alyrn::uring::detail::Op op;
  op.kind = alyrn::uring::detail::OpKind::kAcceptComplete;

  return Expect(op.TryRecordCqeCompletion(42), "Accept CQE must settle its physical request") &&
         Expect(op.CqeCompletionRecorded(), "Accept CQE must settle its physical slot") &&
         Expect(!op.CoupledResultReady(),
                "a raw Accept CQE must not bypass logical stream construction") &&
         Expect(op.TryAuthorizeCoupledResult(),
                "the Accept adapter must authorize its constructed logical result") &&
         Expect(op.TryAuthorizeCoupledRelease(),
                "Accept release must follow logical stream construction") &&
         Expect(op.TryAuthorizeCoupledContinuation(),
                "Accept continuation must follow resource release");
}

bool TestCompletionModels() {
  using alyrn::uring::detail::CompletionModelFor;
  using alyrn::uring::detail::CompletionModel;
  using alyrn::uring::detail::OpKind;

  bool ok = true;
  ok &=
      Expect(CompletionModelFor(OpKind::kReadComplete) == CompletionModel::kSingleShot,
             "ordinary I/O must use the single-shot completion model");
  ok &= Expect(CompletionModelFor(OpKind::kAcceptSourceComplete) ==
                   CompletionModel::kEventStream,
               "accept sources must use the event-stream completion model");
  ok &= Expect(
      CompletionModelFor(OpKind::kRecvSourceComplete) == CompletionModel::kEventStream,
      "recv sources must use the event-stream completion model");
  ok &= Expect(CompletionModelFor(OpKind::kSendZeroCopyComplete) ==
                   CompletionModel::kSplitRelease,
               "zero-copy send must use the split-release completion model");
  return ok;
}

bool TestCancelCqeClassification() {
  using alyrn::uring::detail::IsExpectedCancelCqeResult;

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
                  TestReusableCoupledLifecycle() && TestConnectCqeRequiresAdapterRefinement() &&
                  TestAcceptCqeRequiresAdapterRefinement() && TestCompletionModels() &&
                  TestCancelCqeClassification() && TestResultStatesRejectInvalidTransitions();
  if (ok) {
    std::puts("luring op smoke: PASS");
    return 0;
  }
  return 1;
}
