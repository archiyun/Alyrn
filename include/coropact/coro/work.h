// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: Work, a vtable-free schedulable unit (a tagged action
// plus an intrusive queue hook the scheduler uses to chain entries), and
// ResumeWork, the adapter whose action resumes a coroutine handle. Work itself
// owns no queue -- the queue belongs to whoever implements the Scheduler.
#pragma once

#include <bit>
#include <cassert>
#include <coroutine>
#include <cstdint>

#include "coropact/ds/intrusive_queue.h"
#include "coropact/utils/macros.h"

namespace coropact::coro {

struct Work : public coropact::ds::QueueNode<Work> {
  COROPACT_DELETE_COPY_MOVE(Work);
  using RunFn = void (*)(Work*) noexcept;

  Work() noexcept = default;

  // Stores a callback in the same word that ResumeWork uses for its coroutine
  // handle. Function pointers and coroutine frame addresses are both at least
  // two-byte aligned on the supported Linux targets, so bit zero identifies
  // the action kind without adding a discriminator byte (and its padding).
  void SetRun(RunFn run_fn) noexcept {
    assert(run_fn != nullptr);
    const auto encoded = std::bit_cast<std::uintptr_t>(run_fn);
    assert((encoded & kResumeTag) == 0 && "Work callback address must be aligned");
    action_ = encoded;
  }

  void Run() noexcept {
    const std::uintptr_t action = action_;
    if ((action & kResumeTag) != 0) {
      auto* address = reinterpret_cast<void*>(action & ~kResumeTag);
      auto handle = std::coroutine_handle<>::from_address(address);
      assert(handle && "ResumeWork requires a valid coroutine handle.");
      assert(!handle.done() && "cannot resume a completed coroutine.");
      handle.resume();
      return;
    }

    const auto run_fn = std::bit_cast<RunFn>(action);
    assert(run_fn && "Work::Run is not set");
    run_fn(this);
  }

  void SetHandle(std::coroutine_handle<> handle) noexcept {
    auto* address = handle.address();
    const auto encoded = reinterpret_cast<std::uintptr_t>(address);
    assert((encoded & kResumeTag) == 0 && "coroutine frame address must be aligned");
    action_ = encoded | kResumeTag;
  }

  [[nodiscard]]
  std::coroutine_handle<> Handle() const noexcept {
    assert(IsResume());
    return std::coroutine_handle<>::from_address(reinterpret_cast<void*>(action_ & ~kResumeTag));
  }

  [[nodiscard]]
  bool HasHandle() const noexcept {
    return IsResume() && (action_ & ~kResumeTag) != 0;
  }

  void ClearHandle() noexcept { action_ = kResumeTag; }

private:
  static constexpr std::uintptr_t kResumeTag = 1;

  [[nodiscard]]
  bool IsResume() const noexcept { return (action_ & kResumeTag) != 0; }

  static_assert(sizeof(RunFn) == sizeof(std::uintptr_t));
  std::uintptr_t action_{0};
};

using WorkQueue = coropact::ds::IntrusiveQueue<Work>;

// A Work that resumes a coroutine. This is the only place Work meets a frame.
struct ResumeWork : public Work {
  COROPACT_DELETE_COPY_MOVE(ResumeWork);

  ResumeWork() noexcept { ClearHandle(); }
  explicit ResumeWork(std::coroutine_handle<> handle) noexcept { SetHandle(handle); }
};

static_assert(sizeof(ResumeWork) == sizeof(Work));
static_assert(sizeof(Work) == sizeof(void*) + sizeof(std::uintptr_t));

}  // namespace coropact::coro
