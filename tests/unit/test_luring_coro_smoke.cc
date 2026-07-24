// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <liburing.h>

#include <coroutine>
#include <expected>
#include <iostream>
#include <optional>
#include <system_error>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/op.h"
#include "coropact/luring/options.h"

namespace {

class NopAwaiter {
public:
  explicit NopAwaiter(coropact::luring::LUringLoop& loop) noexcept : loop_(&loop) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    op_.continuation_ = continuation;
    op_.resume_work.handle = continuation;

    auto submitted = loop_->SubmitOp(&op_, [](io_uring_sqe* sqe) noexcept {
      io_uring_prep_nop(sqe);
    });
    if (!submitted.has_value()) {
      result_.emplace(std::unexpected(submitted.error()));
      return false;
    }

    return true;
  }

  coropact::base::Result<int> await_resume() noexcept {
    if (result_.has_value()) {
      return std::move(*result_);
    }
    if (!op_.result.has_value()) {
      return std::unexpected(op_.result.error());
    }
    if (*op_.result < 0) {
      return std::unexpected(coropact::base::make_neg_errno(*op_.result));
    }
    return *op_.result;
  }

private:
  coropact::luring::LUringLoop* loop_;
  coropact::luring::LUringOp op_{.kind = coropact::luring::LUringOpKind::kTimeout};
  std::optional<coropact::base::Result<int>> result_;
};

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool IsEnvironmentSkip(coropact::base::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

struct CompletionCounter {
  int count{0};
};

void CountCompletion(coropact::luring::LUringOp* op) noexcept {
  auto* counter = static_cast<CompletionCounter*>(op->owner);
  ++counter->count;
}

bool CheckSingleShotCompletion() {
  coropact::luring::LUringOp op{
      .kind = coropact::luring::LUringOpKind::kNop,
  };
  CompletionCounter counter;
  op.owner = &counter;
  op.on_complete = &CountCompletion;

  const bool first = op.Complete(17);
  const bool second = op.Complete(23);

  return Check(first, "first completion should be accepted") &&
         Check(!second, "duplicate completion should be rejected") &&
         Check(op.completed, "operation should remain completed") &&
         Check(op.result.has_value(), "first completion should store a result") &&
         Check(*op.result == 17, "duplicate completion must not overwrite the result") &&
         Check(counter.count == 1, "completion hook must run once");
}

coropact::coro::Task<void> AwaitNop(coropact::luring::LUringLoop* loop,
                                std::optional<coropact::base::Result<int>>* out,
                                bool* resumed_with_scheduler) {
  auto result = co_await NopAwaiter(*loop);
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == loop;
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
  if (!Check(loop.IsDrained(), "fresh loop should be drained")) {
    return false;
  }

  std::optional<coropact::base::Result<int>> result;
  bool resumed_with_scheduler = false;

  coropact::coro::Spawn(loop, AwaitNop(&loop, &result, &resumed_with_scheduler)).Detach();

  loop.RunReady();

  if (!Check(loop.PendingSubmitCount() == 1, "NOP should be pending submit after suspension") ||
      !Check(loop.InflightCount() == 0, "NOP should not be inflight before submit") ||
      !Check(!loop.IsDrained(), "loop should not be drained before NOP completion")) {
    return false;
  }

  auto completions = loop.WaitCompletions();
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  if (!Check(loop.PendingSubmitCount() == 0, "pending submit should be empty after wait") ||
      !Check(loop.InflightCount() == 0, "inflight should be empty after NOP CQE") ||
      !Check(loop.IsDrained(), "loop should be drained after NOP CQE")) {
    return false;
  }

  loop.RunReady();

  return Check(*completions >= 1, "NOP did not produce a completion") &&
         Check(result.has_value(), "coroutine did not resume") &&
         Check(result->has_value(), "NOP returned an error") &&
         Check(**result == 0, "NOP result must be zero") &&
         Check(resumed_with_scheduler, "coroutine resumed without current scheduler");
}

}  // namespace

int main() {
  if (!CheckSingleShotCompletion()) return 1;
  if (!CheckNopResumesCoroutine()) return 1;
  std::cout << "luring coro smoke: PASS\n";
  return 0;
}
