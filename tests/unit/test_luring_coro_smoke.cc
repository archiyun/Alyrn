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
#include <system_error>
#include <thread>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/detail/loop_access.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/options.h"
#include "coropact/luring/stream.h"
#include "coropact/io/loop.h"

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

  coropact::base::Result<int> await_resume() noexcept {
    if (result_.has_value()) {
      return std::move(*result_);
    }
    if (!op_.result.HasValue()) {
      return std::unexpected(op_.result.Error());
    }
    if (*op_.result < 0) {
      return std::unexpected(coropact::base::MakeNegErrno(*op_.result));
    }
    return *op_.result;
  }

private:
  coropact::luring::LUringLoop* loop_;
  coropact::luring::detail::LUringOp op_{coropact::luring::detail::LUringOpKind::kNop};
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

coropact::coro::DetachedTask AwaitNop(coropact::luring::LUringLoop* loop,
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
  if (!Check(coropact::luring::detail::LoopAccess::IsDrained(loop), "fresh loop should be drained")) {
    return false;
  }

  std::optional<coropact::base::Result<int>> result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(loop, AwaitNop(&loop, &result, &resumed_with_scheduler));

  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(coropact::luring::detail::LoopAccess::PendingSubmitCount(loop) == 1, "NOP should be pending submit after suspension") ||
      !Check(coropact::luring::detail::LoopAccess::InflightCount(loop) == 0, "NOP should not be inflight before submit") ||
      !Check(!coropact::luring::detail::LoopAccess::IsDrained(loop), "loop should not be drained before NOP completion")) {
    return false;
  }

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  if (!Check(coropact::luring::detail::LoopAccess::PendingSubmitCount(loop) == 0, "pending submit should be empty after wait") ||
      !Check(coropact::luring::detail::LoopAccess::InflightCount(loop) == 0, "inflight should be empty after NOP CQE") ||
      !Check(!coropact::luring::detail::LoopAccess::IsDrained(loop), "completion should queue coroutine resume work")) {
    return false;
  }

  coropact::luring::detail::LoopAccess::RunReady(loop);

  return Check(coropact::luring::detail::LoopAccess::IsDrained(loop), "loop should be drained after coroutine resume") &&
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
    coropact::luring::LUringStream* stream,
    std::array<std::byte, 16>* buffer,
    std::optional<coropact::base::Result<std::size_t>>* result,
    bool* resumed_with_scheduler) {
  auto read = co_await stream->ReadSome(*buffer);
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == stream->Loop();
  result->emplace(std::move(read));
}

bool CheckLoopStopDrainsPendingRead() {
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
  std::optional<coropact::base::Result<std::size_t>> result;
  bool resumed_with_scheduler = false;
  coropact::coro::SpawnDetach(
      loop, AwaitPendingRead(&stream, &buffer, &result, &resumed_with_scheduler));

  std::jthread stopper([&loop] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    loop.RequestStop();
  });
  loop.Run();
  stopper.join();
  ::close(fds[1]);

  return Check(loop.State() == coropact::io::LoopState::kStopped,
               "stopped loop did not report kStopped") &&
         Check(coropact::luring::detail::LoopAccess::IsDrained(loop),
               "stopped loop retained pending ring work") &&
         Check(result.has_value(), "stopped loop did not resume the pending read") &&
         Check(!result->has_value(), "stopped loop unexpectedly completed the read") &&
         Check(result->error() == std::errc::operation_canceled,
               "stopped loop did not cancel the pending read") &&
         Check(resumed_with_scheduler,
               "stopped loop resumed the read without scheduler affinity");
}

}  // namespace

int main() {
  if (!CheckNopResumesCoroutine()) return 1;
  if (!CheckCrossThreadRequestStopWakesRing()) return 1;
  if (!CheckLoopStopDrainsPendingRead()) return 1;
  std::cout << "luring coro smoke: PASS\n";
  return 0;
}
