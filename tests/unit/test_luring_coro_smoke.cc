// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <liburing.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <coroutine>
#include <expected>
#include <iostream>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/result.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/io/loop.h"
#include "coropact/luring/detail/loop_access.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/options.h"
#include "coropact/luring/stream.h"
#include "coropact/operation/detail/scheduler_continuation.h"

namespace {

class NopAwaiter {
public:
  explicit NopAwaiter(coropact::luring::LUringLoop& loop) noexcept : loop_(&loop) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    op_.resume_work.SetHandle(continuation);

    auto submitted = coropact::luring::detail::LoopAccess::SubmitOp(
        *loop_, &op_, [](io_uring_sqe* sqe) noexcept { io_uring_prep_nop(sqe); });
    if (!submitted.has_value()) {
      result_.emplace(std::unexpected(submitted.error()));
      return false;
    }

    return true;
  }

  coropact::Result<int> await_resume() noexcept {
    if (result_.has_value()) {
      return std::move(*result_);
    }
    COROPACT_CHECK(op_.result.HasValue(), "NopAwaiter resumed before its CQE result was ready");
    if (*op_.result < 0) {
      return std::unexpected(coropact::NegErrno(*op_.result));
    }
    return *op_.result;
  }

private:
  coropact::luring::LUringLoop* loop_;
  coropact::luring::detail::LUringOp op_{coropact::luring::detail::LUringOpKind::kNop};
  std::optional<coropact::Result<int>> result_;
};

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

class AppendOrderWork final : public coropact::coro::Work {
public:
  AppendOrderWork(std::string* order, char marker) noexcept : order_(order), marker_(marker) {
    SetRun(&RunAppend);
  }

private:
  static void RunAppend(coropact::coro::Work* work) noexcept {
    auto* self = static_cast<AppendOrderWork*>(work);
    self->order_->push_back(self->marker_);
  }

  std::string* order_;
  char marker_;
};

class SuspendOnContinuation final {
public:
  explicit SuspendOnContinuation(
      coropact::operation::detail::SchedulerContinuation* continuation) noexcept
      : continuation_(continuation) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  bool await_suspend(std::coroutine_handle<> handle) noexcept {
    continuation_->Bind(handle);
    return true;
  }

  void await_resume() const noexcept {}

private:
  coropact::operation::detail::SchedulerContinuation* continuation_;
};

coropact::coro::DetachedTask AwaitCompletionQueue(
    coropact::operation::detail::SchedulerContinuation* continuation,
    coropact::luring::LUringLoop* loop, std::string* order, bool* resumed_with_scheduler) {
  co_await SuspendOnContinuation(continuation);
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == loop;
  order->push_back('C');
}

bool CheckCompletionQueuePrecedesNormalReadyWork() {
  coropact::luring::LUringLoop loop;
  coropact::operation::detail::SchedulerContinuation continuation;
  std::string order;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(
      loop, AwaitCompletionQueue(&continuation, &loop, &order, &resumed_with_scheduler));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  AppendOrderWork normal_work{&order, 'N'};
  loop.Schedule(&normal_work);
  coropact::luring::detail::LoopAccess::ScheduleCompletion(loop, continuation);
  coropact::luring::detail::LoopAccess::RunReady(loop);

  return Check(order == "CN", "completion queue work must precede normal ready work") &&
         Check(resumed_with_scheduler,
               "completion queue continuation must retain its loop scheduler") &&
         Check(coropact::luring::detail::LoopAccess::IsDrained(loop),
               "completion priority test must drain the loop");
}

bool IsEnvironmentSkip(coropact::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

coropact::coro::DetachedTask AwaitNop(coropact::luring::LUringLoop* loop,
                                      std::optional<coropact::Result<int>>* out,
                                      bool* resumed_with_scheduler) {
  auto result = co_await NopAwaiter(*loop);
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == loop;
  out->emplace(std::move(result));
}

bool CheckNopResumesCoroutine() {
  coropact::luring::LUringLoop loop;

  coropact::luring::LUringOptions options;
  options.entries = 8;
  options.submit_batch = 1;

  auto init = loop.Init(options);
  if (!init.has_value()) {
    if (IsEnvironmentSkip(init.error())) {
      std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: LUringLoop init failed: " << init.error().message() << '\n';
    return false;
  }
  if (!Check(loop.IsInLoopThread(), "loop should be bound to the creating thread")) {
    return false;
  }
  if (!Check(coropact::luring::detail::LoopAccess::IsDrained(loop),
             "fresh loop should be drained")) {
    return false;
  }

  std::optional<coropact::Result<int>> result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(loop, AwaitNop(&loop, &result, &resumed_with_scheduler));

  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(coropact::luring::detail::LoopAccess::PendingSubmitCount(loop) == 1,
             "NOP should be pending submit after suspension") ||
      !Check(coropact::luring::detail::LoopAccess::InflightCount(loop) == 0,
             "NOP should not be inflight before submit") ||
      !Check(!coropact::luring::detail::LoopAccess::IsDrained(loop),
             "loop should not be drained before NOP completion")) {
    return false;
  }

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  if (!Check(coropact::luring::detail::LoopAccess::PendingSubmitCount(loop) == 0,
             "pending submit should be empty after wait") ||
      !Check(coropact::luring::detail::LoopAccess::InflightCount(loop) == 0,
             "inflight should be empty after NOP CQE") ||
      !Check(!coropact::luring::detail::LoopAccess::IsDrained(loop),
             "completion should queue coroutine resume work")) {
    return false;
  }

  coropact::luring::detail::LoopAccess::RunReady(loop);

  return Check(coropact::luring::detail::LoopAccess::IsDrained(loop),
               "loop should be drained after coroutine resume") &&
         Check(*completions >= 1, "NOP did not produce a completion") &&
         Check(result.has_value(), "coroutine did not resume") &&
         Check(result->has_value(), "NOP returned an error") &&
         Check(**result == 0, "NOP result must be zero") &&
         Check(resumed_with_scheduler, "coroutine resumed without current scheduler");
}

bool CheckCrossThreadRequestStopWakesRing() {
  coropact::luring::LUringLoop loop;
  coropact::luring::LUringOptions options;
  options.entries = 8;

  auto init = loop.Init(options);
  if (!init.has_value()) {
    if (IsEnvironmentSkip(init.error())) {
      std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: LUringLoop init failed: " << init.error().message() << '\n';
    return false;
  }

  std::jthread stopper([&loop] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    loop.RequestStop();
  });

  const auto start = std::chrono::steady_clock::now();
  loop.Run();
  stopper.join();
  const auto elapsed = std::chrono::steady_clock::now() - start;

  return Check(loop.State() == coropact::io::LoopState::kStopped,
               "LUringLoop should reach stopped after RequestStop") &&
         Check(elapsed < std::chrono::seconds(1),
               "RequestStop should wake the io_uring wait promptly");
}

coropact::coro::DetachedTask AwaitPendingRead(
    coropact::luring::LUringStream* stream, std::array<std::byte, 16>* buffer,
    std::optional<coropact::Result<std::size_t>>* result, bool* resumed_with_scheduler) {
  auto read = co_await stream->ReadSome(*buffer);
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == stream->Loop();
  result->emplace(std::move(read));
}

enum class StopDrainFailure {
  kNone,
  kCancelPreparation,
  kFlushSubmit,
};

bool RunLoopStopDrainScenario(StopDrainFailure injected_failure) {
  int fds[2] = {-1, -1};
  if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) != 0) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  coropact::luring::LUringLoop loop;
  coropact::luring::LUringOptions options;
  options.entries = 8;

  auto init = loop.Init(options);
  if (!init.has_value()) {
    ::close(fds[0]);
    ::close(fds[1]);
    if (IsEnvironmentSkip(init.error())) {
      std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: LUringLoop init failed: " << init.error().message() << '\n';
    return false;
  }

  coropact::luring::LUringStream stream(&loop, fds[0], coropact::net::Endpoint(0));
  std::array<std::byte, 16> buffer{};
  std::optional<coropact::Result<std::size_t>> result;
  bool resumed_with_scheduler = false;
  coropact::coro::SpawnDetach(loop,
                              AwaitPendingRead(&stream, &buffer, &result, &resumed_with_scheduler));

  const bool stop_before_run = injected_failure == StopDrainFailure::kFlushSubmit;

#if defined(COROPACT_ENABLE_TEST_HOOKS)
  if (injected_failure != StopDrainFailure::kNone) {
    // Ensure the read request itself has reached the ring before failing the
    // later shutdown action. This keeps the target read live while the drain
    // path handles the injected failure.
    coropact::luring::detail::LoopAccess::RunReady(loop);
    auto flushed = coropact::luring::detail::LoopAccess::FlushSubmit(loop);
    if (!flushed.has_value()) {
      std::cout << "FAIL: initial read submit failed: " << flushed.error().message() << '\n';
      ::close(fds[1]);
      return false;
    }
    if (injected_failure == StopDrainFailure::kCancelPreparation) {
      // The next local operation preparation is the loop-wide cancel SQE.
      loop.FailNextSubmissionsForTesting(1, EIO);
    } else {
      // Pre-request stop so Run() enters DrainStoppedOperations() directly;
      // the next flush then targets the cancel SQE prepared by that drain.
      loop.FailNextFlushesForTesting(1, EIO);
    }
  }
#else
  (void)(injected_failure);
#endif

  if (stop_before_run) {
    loop.RequestStop();
  }

  std::optional<std::jthread> stopper;
  if (!stop_before_run) {
    stopper.emplace([&loop] {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      loop.RequestStop();
    });
  }
  loop.Run();
  if (stopper.has_value()) {
    stopper->join();
  }
  ::close(fds[1]);

  return Check(loop.State() == coropact::io::LoopState::kStopped,
               "stopped loop did not report kStopped") &&
         Check(coropact::luring::detail::LoopAccess::IsDrained(loop),
               "stopped loop retained pending ring work") &&
         Check(result.has_value(), "stopped loop did not resume the pending read") &&
         Check(!result->has_value(), "stopped loop unexpectedly completed the read") &&
         Check(result->error() == std::errc::operation_canceled,
               "stopped loop did not cancel the pending read") &&
         Check(resumed_with_scheduler, "stopped loop resumed the read without scheduler affinity");
}

bool CheckLoopStopDrainsPendingRead() { return RunLoopStopDrainScenario(StopDrainFailure::kNone); }

#if defined(COROPACT_ENABLE_TEST_HOOKS)
bool CheckLoopStopRetriesCancelSubmitFailure() {
  return RunLoopStopDrainScenario(StopDrainFailure::kCancelPreparation);
}

bool CheckLoopStopRetriesFlushSubmitFailure() {
  return RunLoopStopDrainScenario(StopDrainFailure::kFlushSubmit);
}
#endif

}  // namespace

int main() {
  if (!CheckCompletionQueuePrecedesNormalReadyWork()) return 1;
  if (!CheckNopResumesCoroutine()) return 1;
  if (!CheckCrossThreadRequestStopWakesRing()) return 1;
  if (!CheckLoopStopDrainsPendingRead()) return 1;
#if defined(COROPACT_ENABLE_TEST_HOOKS)
  if (!CheckLoopStopRetriesCancelSubmitFailure()) return 1;
  if (!CheckLoopStopRetriesFlushSubmitFailure()) return 1;
#endif
  std::cout << "luring coro smoke: PASS\n";
  return 0;
}
