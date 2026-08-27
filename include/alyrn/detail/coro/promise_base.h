// SPDX-License-Identifier: MIT
//
// Single responsibility: the backend-neutral coroutine contract every Task
// promise shares -- lazy start, symmetric-transfer completion, fatal-on-throw.
// No scheduling, IO, join machinery, value storage, or allocator lives here.
#pragma once

#include <coroutine>
#include <exception>

#include "alyrn/coro/frame_allocator.h"
#include "alyrn/detail/utils/macros.h"

namespace alyrn::coro::detail {

class PromiseBase : public FrameAllocationSupport {
public:
  ALYRN_DELETE_COPY_MOVE(PromiseBase);

  // Lazy: the body does not run until a consumer resumes the frame.
  auto initial_suspend() const noexcept { return std::suspend_always{}; }

  // On completion, tail-call into the awaiting coroutine. When nothing awaits,
  // continuation_ stays std::noop_coroutine and resume returns to its caller.
  struct FinalAwaiter {
    std::coroutine_handle<> continuation;

    bool await_ready() const noexcept { return false; }

    [[nodiscard]]
    std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept {
      return continuation;
    }

    void await_resume() const noexcept {}
  };

  FinalAwaiter final_suspend() noexcept { return FinalAwaiter{continuation_}; }

  // Exceptions are banned project-wide; reaching here is an unrecoverable bug.
  void unhandled_exception() noexcept { std::terminate(); }

  void SetContinuation(std::coroutine_handle<> continuation) noexcept {
    continuation_ = continuation ? continuation : std::noop_coroutine();
  }

  [[nodiscard]]
  std::coroutine_handle<> Continuation() const noexcept {
    return continuation_;
  }

protected:
  PromiseBase() = default;
  ~PromiseBase() = default;

private:
  std::coroutine_handle<> continuation_{std::noop_coroutine()};
};

}  // namespace alyrn::coro::detail
