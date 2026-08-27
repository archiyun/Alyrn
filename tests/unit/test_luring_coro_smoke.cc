// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <coroutine>
#include <expected>
#include <iostream>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

#include "alyrn/detail/base/check.h"
#include "alyrn/result.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/detached_task.h"
#include "alyrn/coro/work.h"
#include "alyrn/io/loop.h"
#include "alyrn/detail/uring/loop_access.h"
#include "alyrn/detail/uring/op.h"
#include "alyrn/detail/uring/sqe_prep.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/options.h"
#include "alyrn/uring/stream.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/detail/operation/scheduler_continuation.h"

namespace {

class NopAwaiter {
public:
  explicit NopAwaiter(alyrn::uring::Loop& loop) noexcept : loop_(&loop) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    op_.resume_work.SetHandle(continuation);

    auto submitted = alyrn::uring::detail::LoopAccess::SubmitOp(
        *loop_, &op_, alyrn::uring::detail::PrepareNop());
    if (!submitted.has_value()) {
      result_.emplace(std::unexpected(submitted.error()));
      return false;
    }

    return true;
  }

  alyrn::Result<int> await_resume() noexcept {
    if (result_.has_value()) {
      return std::move(*result_);
    }
    ALYRN_CHECK(op_.result.HasValue(), "NopAwaiter resumed before its CQE result was ready");
    if (*op_.result < 0) {
      return std::unexpected(alyrn::NegErrno(*op_.result));
    }
    return *op_.result;
  }

private:
  alyrn::uring::Loop* loop_;
  alyrn::uring::detail::Op op_{alyrn::uring::detail::OpKind::kNop};
  std::optional<alyrn::Result<int>> result_;
};

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

class AppendOrderWork final : public alyrn::coro::Work {
public:
  AppendOrderWork(std::string* order, char marker) noexcept : order_(order), marker_(marker) {
    SetRun(&RunAppend);
  }

private:
  static void RunAppend(alyrn::coro::Work* work) noexcept {
    auto* self = static_cast<AppendOrderWork*>(work);
    self->order_->push_back(self->marker_);
  }

  std::string* order_;
  char marker_;
};

class SuspendOnContinuation final {
public:
  explicit SuspendOnContinuation(
      alyrn::detail::operation::SchedulerContinuation* continuation) noexcept
      : continuation_(continuation) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> handle) noexcept {
    continuation_->Bind(handle);
    return true;
  }

  void await_resume() const noexcept {}

private:
  alyrn::detail::operation::SchedulerContinuation* continuation_;
};

alyrn::coro::DetachedTask AwaitCompletionQueue(
    alyrn::detail::operation::SchedulerContinuation* continuation,
    alyrn::uring::Loop* loop, std::string* order, bool* resumed_with_scheduler) {
  co_await SuspendOnContinuation(continuation);
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == loop;
  order->push_back('C');
}

bool CheckCompletionQueuePrecedesNormalReadyWork() {
  /* A Work* is non-owning, so anything the loop may still hold at destruction
   * must outlive it. Declaring these before the loop makes them destroyed
   * after it, which keeps an early return from leaving the loop's queue
   * pointing at dead stack. */
  std::string order;
  bool resumed_with_scheduler = false;
  alyrn::detail::operation::SchedulerContinuation continuation;
  AppendOrderWork normal_work{&order, 'N'};

  alyrn::uring::Loop loop;

  alyrn::coro::SpawnDetach(
      loop, AwaitCompletionQueue(&continuation, &loop, &order, &resumed_with_scheduler));
  alyrn::uring::detail::LoopAccess::RunReady(loop);

  loop.Schedule(&normal_work);
  alyrn::uring::detail::LoopAccess::ScheduleCompletion(loop, continuation);

  /* RunReady() stops a turn once the wall-clock fairness budget is spent, so
   * one turn is not guaranteed to drain both queues on a slow or instrumented
   * build. Ordering is still asserted across turns: the completion queue is
   * drained ahead of normal ready work within every turn. */
  for (int turn = 0; turn < 8 && !alyrn::uring::detail::LoopAccess::IsDrained(loop); ++turn) {
    alyrn::uring::detail::LoopAccess::RunReady(loop);
  }

  return Check(order == "CN", "completion queue work must precede normal ready work") &&
         Check(resumed_with_scheduler,
               "completion queue continuation must retain its loop scheduler") &&
         Check(alyrn::uring::detail::LoopAccess::IsDrained(loop),
               "completion priority test must drain the loop");
}

bool IsEnvironmentSkip(alyrn::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

alyrn::coro::DetachedTask AwaitNop(alyrn::uring::Loop* loop,
                                      std::optional<alyrn::Result<int>>* out,
                                      bool* resumed_with_scheduler) {
  auto result = co_await NopAwaiter(*loop);
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == loop;
  out->emplace(std::move(result));
}

bool CheckNopResumesCoroutine() {
  alyrn::uring::Loop loop;

  alyrn::uring::Options options;
  options.entries = 8;

  auto init = loop.Init(options);
  if (!init.has_value()) {
    if (IsEnvironmentSkip(init.error())) {
      std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: Loop init failed: " << init.error().message() << '\n';
    return false;
  }
  if (!Check(loop.IsInLoopThread(), "loop should be bound to the creating thread")) {
    return false;
  }
  if (!Check(alyrn::uring::detail::LoopAccess::IsDrained(loop),
             "fresh loop should be drained")) {
    return false;
  }

  std::optional<alyrn::Result<int>> result;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(loop, AwaitNop(&loop, &result, &resumed_with_scheduler));

  alyrn::uring::detail::LoopAccess::RunReady(loop);

  if (!Check(alyrn::uring::detail::LoopAccess::PendingSubmitCount(loop) == 1,
             "NOP should be pending submit after suspension") ||
      !Check(alyrn::uring::detail::LoopAccess::InflightCount(loop) == 0,
             "NOP should not be inflight before submit") ||
      !Check(!alyrn::uring::detail::LoopAccess::IsDrained(loop),
             "loop should not be drained before NOP completion")) {
    return false;
  }

  auto completions = alyrn::uring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  if (!Check(alyrn::uring::detail::LoopAccess::PendingSubmitCount(loop) == 0,
             "pending submit should be empty after wait") ||
      !Check(alyrn::uring::detail::LoopAccess::InflightCount(loop) == 0,
             "inflight should be empty after NOP CQE") ||
      !Check(!alyrn::uring::detail::LoopAccess::IsDrained(loop),
             "completion should queue coroutine resume work")) {
    return false;
  }

  alyrn::uring::detail::LoopAccess::RunReady(loop);

  return Check(alyrn::uring::detail::LoopAccess::IsDrained(loop),
               "loop should be drained after coroutine resume") &&
         Check(*completions >= 1, "NOP did not produce a completion") &&
         Check(result.has_value(), "coroutine did not resume") &&
         Check(result->has_value(), "NOP returned an error") &&
         Check(**result == 0, "NOP result must be zero") &&
         Check(resumed_with_scheduler, "coroutine resumed without current scheduler");
}

bool CheckCrossThreadRequestStopWakesRing() {
  alyrn::uring::Loop loop;
  alyrn::uring::Options options;
  options.entries = 8;

  auto init = loop.Init(options);
  if (!init.has_value()) {
    if (IsEnvironmentSkip(init.error())) {
      std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: Loop init failed: " << init.error().message() << '\n';
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

  return Check(loop.State() == alyrn::io::LoopState::kStopped,
               "Loop should reach stopped after RequestStop") &&
         Check(elapsed < std::chrono::seconds(1),
               "RequestStop should wake the io_uring wait promptly");
}

alyrn::coro::DetachedTask AwaitPendingRead(
    alyrn::uring::Stream* stream, std::array<std::byte, 16>* buffer,
    std::optional<alyrn::Result<std::size_t>>* result, bool* resumed_with_scheduler) {
  auto read = co_await stream->ReadSome(*buffer);
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == stream->OwnerLoop();
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

  alyrn::uring::Loop loop;
  alyrn::uring::Options options;
  options.entries = 8;

  auto init = loop.Init(options);
  if (!init.has_value()) {
    ::close(fds[0]);
    ::close(fds[1]);
    if (IsEnvironmentSkip(init.error())) {
      std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: Loop init failed: " << init.error().message() << '\n';
    return false;
  }

  alyrn::uring::Stream stream(&loop, fds[0], alyrn::net::Endpoint(0));
  std::array<std::byte, 16> buffer{};
  std::optional<alyrn::Result<std::size_t>> result;
  bool resumed_with_scheduler = false;
  alyrn::coro::SpawnDetach(loop,
                              AwaitPendingRead(&stream, &buffer, &result, &resumed_with_scheduler));

  const bool stop_before_run = injected_failure == StopDrainFailure::kFlushSubmit;


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

  return Check(loop.State() == alyrn::io::LoopState::kStopped,
               "stopped loop did not report kStopped") &&
         Check(alyrn::uring::detail::LoopAccess::IsDrained(loop),
               "stopped loop retained pending ring work") &&
         Check(result.has_value(), "stopped loop did not resume the pending read") &&
         Check(!result->has_value(), "stopped loop unexpectedly completed the read") &&
         Check(result->error() == std::errc::operation_canceled,
               "stopped loop did not cancel the pending read") &&
         Check(resumed_with_scheduler, "stopped loop resumed the read without scheduler affinity");
}

bool CheckLoopStopDrainsPendingRead() { return RunLoopStopDrainScenario(StopDrainFailure::kNone); }


}  // namespace

int main() {
  if (!CheckCompletionQueuePrecedesNormalReadyWork()) return 1;
  if (!CheckNopResumesCoroutine()) return 1;
  if (!CheckCrossThreadRequestStopWakesRing()) return 1;
  if (!CheckLoopStopDrainsPendingRead()) return 1;
  std::cout << "luring coro smoke: PASS\n";
  return 0;
}
