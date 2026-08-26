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

#include "coropact/base/check.h"
#include "coropact/coro/frame_allocator.h"
#include "coropact/coro/work.h"
#include "coropact/utils/macros.h"

namespace coropact::coro {

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
    COROPACT_CHECK(work != nullptr, "Scheduler::Run received null work");
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
    while (Work* work = batch.PopFront()) {
      RunInExecutionScope(work);
    }
  }

  [[nodiscard]]
  static Scheduler* TryCurrent() noexcept {
    return current_;
  }
  static Scheduler& RequireCurrent() noexcept {
    COROPACT_CHECK(current_ != nullptr, "no current scheduler set for this thread");
    return *current_;
  }

protected:
  // Keeps the coroutine execution context active while a concrete scheduler
  // drains a non-WorkQueue source. It is protected so only scheduler
  // implementations can amortize TLS/resource selection across a batch; work
  // execution itself must still go through Run() or RunBatch().
  class ExecutionScope {
  public:
    explicit ExecutionScope(Scheduler& scheduler) noexcept
        : scheduler_scope_(scheduler), frame_scope_(scheduler.FrameResource()) {}

    COROPACT_DELETE_COPY_MOVE(ExecutionScope);

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

      COROPACT_DELETE_COPY_MOVE(CurrentScope);

    private:
      Scheduler* previous_;
      bool changed_;
    };

    CurrentScope scheduler_scope_;
    FrameAllocatorScope frame_scope_;
  };

  // Runs a work item under an already-active ExecutionScope for this
  // scheduler. This exists for schedulers such as Loop that select work
  // from multiple owner-local queues and therefore cannot use RunBatch().
  // Callers must create an ExecutionScope that outlives this call.
  void RunInExecutionScope(Work* work) noexcept {
    COROPACT_CHECK(work != nullptr, "Scheduler::RunInExecutionScope received null work");
    COROPACT_DCHECK(TryCurrent() == this,
                    "Scheduler::RunInExecutionScope requires this scheduler context");
    COROPACT_DCHECK(
        FrameAllocatorScope::TryCurrent() == detail::NormalizeFrameResource(frame_resource_),
        "Scheduler::RunInExecutionScope requires this frame resource");
    work->Run();
  }

  explicit Scheduler(std::pmr::memory_resource* frame_resource = nullptr) noexcept
      : frame_resource_(frame_resource) {}

  void SetFrameResource(std::pmr::memory_resource* frame_resource) noexcept {
    frame_resource_ = frame_resource;
  }

  COROPACT_DELETE_COPY_MOVE(Scheduler);

private:
  static thread_local Scheduler* current_;
  std::pmr::memory_resource* frame_resource_{nullptr};
};

}  // namespace coropact::coro
