// SPDX-License-Identifier: MIT
//
// Single responsibility: the Scheduler abstraction. Schedule(Work*) is the one
// coarse-grained virtual boundary this module allows (the no-vtable-on-the-hot-
// path rule covers per-resume/per-await code, not this submission edge).
// Run()/RunBatch() publish the per-thread active scheduler so an awaiter can
// re-submit work without threading a pointer through every frame. Concrete
// schedulers must execute work through one of these wrappers; the coro module
// never implements a concrete scheduler or owns a queue.
#pragma once

#include "alyrn/detail/base/check.h"
#include "alyrn/coro/frame_allocator.h"
#include "alyrn/coro/work.h"
#include "alyrn/detail/utils/macros.h"

namespace alyrn::coro {

class Scheduler {
public:
  virtual ~Scheduler() = default;

  [[nodiscard]]
  std::pmr::memory_resource* FrameResource() const noexcept {
    return frame_resource_;
  }

  // Preconditions:
  // - work != nullptr
  // - work has a configured action (SetRun() or a ResumeWork handle)
  // - work is not already enqueued
  // - work stays alive until it is run or cancelled by owner-side protocol
  virtual void Schedule(Work* work) noexcept = 0;

  // Runs a work item with this scheduler's frame resource and execution context
  // active. Concrete schedulers should use this wrapper instead of calling
  // Work::Run() directly so coroutine frames and awaiters created during a
  // resume observe the selected resource and Scheduler::TryCurrent().
  void Run(Work* work) noexcept {
    ALYRN_CHECK(work != nullptr, "Scheduler::Run received null work");
    ExecutionScope execution_scope{*this};
    RunInExecutionScope(work);
  }

  // Runs an owner-local batch under one scheduler/frame-resource context.
  // Work::Run() may enqueue more work on the scheduler, but those entries stay
  // in the scheduler's queue and are intentionally handled by a later batch.
  // This preserves the single-resume boundary while avoiding a TLS/resource
  // scope transition for every item in a ready queue drain.
  void RunBatch(WorkQueue& batch) noexcept {
    ExecutionScope execution_scope{*this};
    CheckExecutionScope();
    while (Work* work = batch.PopFront()) {
      // ExecutionScope is active for the whole batch, so repeating its TLS
      // validation for every work item only adds hot-path loads. RunBatch is
      // the execution wrapper here; Work::Run() must not escape this scope.
      RunInExecutionScopeUnchecked(work);
    }
  }

  [[nodiscard]]
  static Scheduler* TryCurrent() noexcept {
    return current_;
  }
  static Scheduler& RequireCurrent() noexcept {
    ALYRN_CHECK(current_ != nullptr, "no current scheduler set for this thread");
    return *current_;
  }

protected:
  // Keeps the coroutine execution context active while a concrete scheduler
  // drains a non-WorkQueue source. It is protected so only scheduler
  // implementations can amortize TLS/resource selection across a batch; a
  // concrete scheduler may then call CheckExecutionScope() once and use the
  // unchecked Scheduler wrapper while this scope remains alive.
  class ExecutionScope {
  public:
    explicit ExecutionScope(Scheduler& scheduler) noexcept
        : scheduler_scope_(scheduler), frame_scope_(scheduler.FrameResource()) {}

    ALYRN_DELETE_COPY_MOVE(ExecutionScope);

  private:
    class CurrentScope {
    public:
      explicit CurrentScope(Scheduler& scheduler) noexcept
          : previous_(TryCurrent()), changed_(previous_ != &scheduler) {
        if (changed_) {
          current_ = &scheduler;
        }
      }

      ~CurrentScope() {
        if (changed_) {
          current_ = previous_;
        }
      }

      ALYRN_DELETE_COPY_MOVE(CurrentScope);

    private:
      Scheduler* previous_;
      bool changed_;
    };

    CurrentScope scheduler_scope_;
    FrameAllocatorScope frame_scope_;
  };

  // Runs one work item under an already-active ExecutionScope for this
  // scheduler. This is the checked single-item wrapper; batch schedulers can
  // amortize the same validation with CheckExecutionScope() and the unchecked
  // Scheduler wrapper.
  void RunInExecutionScope(Work* work) noexcept {
    ALYRN_CHECK(work != nullptr, "Scheduler::RunInExecutionScope received null work");
    CheckExecutionScope();
    RunInExecutionScopeUnchecked(work);
  }

  // The caller must have validated the active ExecutionScope and the Work
  // pointer must come from an owner-local non-empty queue. Keeping the raw
  // resume behind this Scheduler wrapper preserves the frame-resource and
  // scheduler-affinity boundary without repeating the checks per item.
  void RunInExecutionScopeUnchecked(Work* work) noexcept {
    work->Run();
  }

  // Validates the owner-local execution context once before a scheduler drains
  // a batch. Keeping this as a separate boundary check lets concrete
  // schedulers execute each Work without repeating the same TLS loads.
  void CheckExecutionScope() const noexcept {
    ALYRN_CHECK(TryCurrent() == this,
                   "Scheduler execution requires this scheduler context");
    ALYRN_CHECK(
        FrameAllocatorScope::TryCurrent() == detail::NormalizeFrameResource(frame_resource_),
        "Scheduler execution requires this frame resource");
  }

  explicit Scheduler(std::pmr::memory_resource* frame_resource = nullptr) noexcept
      : frame_resource_(frame_resource) {}

  void SetFrameResource(std::pmr::memory_resource* frame_resource) noexcept {
    frame_resource_ = frame_resource;
  }

  ALYRN_DELETE_COPY_MOVE(Scheduler);

private:
  static thread_local Scheduler* current_;
  std::pmr::memory_resource* frame_resource_{nullptr};
};

}  // namespace alyrn::coro
