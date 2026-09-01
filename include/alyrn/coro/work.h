// SPDX-License-Identifier: MIT
//
// Single responsibility: Work, a vtable-free schedulable unit (a tagged action
// plus an intrusive queue hook the scheduler uses to chain entries), and
// ResumeWork, the adapter whose action resumes a coroutine handle. Work itself
// owns no queue -- the queue belongs to whoever implements the Scheduler.
#pragma once

#include <bit>
#include <coroutine>
#include <cstdint>

#include "alyrn/detail/check.h"
#include "alyrn/detail/intrusive_queue.h"
#include "alyrn/detail/macros.h"

namespace alyrn::coro {

struct Work : public ::alyrn::detail::QueueNode<Work> {
  ALYRN_DELETE_COPY_MOVE(Work);
  using RunFn = void (*)(Work*) noexcept;

  Work() noexcept = default;

  // Stores a callback in the same word that ResumeWork uses for its coroutine
  // handle. The high bit identifies the action kind without adding a
  // discriminator byte (and its padding). Linux user-space function and frame
  // addresses leave this bit clear on the supported 64-bit targets.
  void SetRun(RunFn run_fn) noexcept {
    ALYRN_CHECK(run_fn != nullptr, "Work::SetRun requires a callback");
    const auto encoded = std::bit_cast<std::uintptr_t>(run_fn);
    ALYRN_CHECK((encoded & kResumeTag) == 0, "Work callback address collides with the resume tag");
    action_ = encoded;
  }

  void Run() noexcept {
    const std::uintptr_t action = action_;
    if ((action & kResumeTag) != 0) {
      auto* address = reinterpret_cast<void*>(action & ~kResumeTag);
      auto handle = std::coroutine_handle<>::from_address(address);
      ALYRN_CHECK(handle, "ResumeWork requires a valid coroutine handle");
      ALYRN_CHECK(!handle.done(), "cannot resume a completed coroutine");
      handle.resume();
      return;
    }

    const auto run_fn = std::bit_cast<RunFn>(action);
    ALYRN_CHECK(run_fn != nullptr, "Work::Run has no configured action");
    run_fn(this);
  }

  void SetHandle(std::coroutine_handle<> handle) noexcept {
    ALYRN_CHECK(handle, "ResumeWork requires a valid coroutine handle");
    auto* address = handle.address();
    const auto encoded = reinterpret_cast<std::uintptr_t>(address);
    ALYRN_CHECK((encoded & kResumeTag) == 0,
                "coroutine frame address collides with the resume tag");
    action_ = encoded | kResumeTag;
  }

  [[nodiscard]]
  std::coroutine_handle<> Handle() const noexcept {
    ALYRN_CHECK(HasHandle(), "ResumeWork has no coroutine handle");
    return std::coroutine_handle<>::from_address(reinterpret_cast<void*>(action_ & ~kResumeTag));
  }

  [[nodiscard]]
  bool HasHandle() const noexcept {
    return IsResume() && (action_ & ~kResumeTag) != 0;
  }

  void ClearHandle() noexcept { action_ = kResumeTag; }

private:
  static constexpr std::uintptr_t kResumeTag = std::uintptr_t{1}
                                               << (sizeof(std::uintptr_t) * 8) - 1;

  bool IsResume() const noexcept { return (action_ & kResumeTag) != 0; }

  static_assert(sizeof(RunFn) == sizeof(std::uintptr_t));
  std::uintptr_t action_{0};
};

using WorkQueue = ::alyrn::detail::IntrusiveQueue<Work>;

// A Work that resumes a coroutine. This is the only place Work meets a frame.
struct ResumeWork : public Work {
  ALYRN_DELETE_COPY_MOVE(ResumeWork);

  ResumeWork() noexcept { ClearHandle(); }
  explicit ResumeWork(std::coroutine_handle<> handle) noexcept { SetHandle(handle); }
};

static_assert(sizeof(ResumeWork) == sizeof(Work));
static_assert(sizeof(Work) == sizeof(void*) + sizeof(std::uintptr_t));

}  // namespace alyrn::coro
