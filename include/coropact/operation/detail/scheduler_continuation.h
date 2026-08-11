// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <type_traits>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/work.h"
#include "coropact/utils/macros.h"

namespace coropact::operation::detail {

// Captures the scheduler affinity and resumable coroutine frame for one
// suspended logical operation. The owning backend calls Schedule() only after
// CompletionGate authorizes a logical result.
//
// This type is thread-confined. A backend must marshal completion onto the
// operation owner before scheduling the continuation. Schedule() may resume
// and destroy the awaiter that owns this object, so callers must not access the
// owner after calling it.
class SchedulerContinuation {
public:
  COROPACT_DELETE_COPY_MOVE(SchedulerContinuation);

  SchedulerContinuation() noexcept = default;

  void Bind(std::coroutine_handle<> handle) noexcept {
    COROPACT_CHECK(!Bound(), "SchedulerContinuation was bound twice");
    COROPACT_CHECK(handle, "SchedulerContinuation requires a valid coroutine handle");
    scheduler_ = coro::Scheduler::TryCurrent();
    COROPACT_CHECK(scheduler_ != nullptr,
                   "SchedulerContinuation requires a current owner scheduler");
    resume_work_.SetHandle(handle);
  }

  [[nodiscard]]
  bool Bound() const noexcept {
    return scheduler_ != nullptr;
  }

  void Schedule() noexcept {
    COROPACT_CHECK(scheduler_ != nullptr, "SchedulerContinuation scheduled before Bind");
    COROPACT_CHECK(resume_work_.HasHandle(),
                   "SchedulerContinuation has no resumable coroutine handle");
    scheduler_->Schedule(&resume_work_);
  }

  // Lets a concrete backend select an owner-local dispatch queue while this
  // type continues to own scheduler affinity and the intrusive ResumeWork.
  // The dispatcher receives the scheduler captured by Bind() and must preserve
  // that affinity. The dispatcher must be noexcept. Like Schedule(), it may
  // resume and destroy the awaiter that owns this continuation; callers must
  // not touch that owner afterward.
  template <class Dispatch>
  void ScheduleWith(Dispatch&& dispatch) noexcept {
    static_assert(std::is_nothrow_invocable_v<Dispatch&&, coro::Scheduler&, coro::Work*>,
                  "SchedulerContinuation dispatcher must be noexcept");
    COROPACT_CHECK(scheduler_ != nullptr, "SchedulerContinuation scheduled before Bind");
    COROPACT_CHECK(resume_work_.HasHandle(),
                   "SchedulerContinuation has no resumable coroutine handle");
    std::forward<Dispatch>(dispatch)(*scheduler_, &resume_work_);
  }

private:
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_;
};

// SchedulerContinuation has exactly the storage that awaiters previously
// carried themselves: one scheduler pointer and one intrusive ResumeWork.
static_assert(sizeof(SchedulerContinuation) == 3 * sizeof(void*));

}  // namespace coropact::operation::detail
