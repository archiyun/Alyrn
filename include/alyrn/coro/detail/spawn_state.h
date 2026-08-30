// SPDX-License-Identifier: MIT
//
// Single responsibility: the atomic lifecycle state shared by a spawned root
// coroutine and its JoinHandle. The state is embedded in SpawnRoot's frame;
// it resolves completion, waiting, and detach races and decides which side
// destroys that frame. The scheduler selected by the parent re-submits a
// parked waiter; it need not be the scheduler that starts the root.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "alyrn/detail/check.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/work.h"

namespace alyrn::coro::detail {

// Manual-lifetime result slot, so a non-default-constructible T still works.
// void collapses to an empty specialization.
template <class T>
class ResultSlot {
public:
  ResultSlot() noexcept {}

  ~ResultSlot() {
    if (engaged_) {
      value_.~T();
    }
  }

  template <class U>
  void Set(U&& value) {
    ALYRN_CHECK(!engaged_, "SpawnState result was stored twice");
    std::construct_at(std::addressof(value_), std::forward<U>(value));
    engaged_ = true;
  }

  T Take() noexcept(std::is_nothrow_move_constructible_v<T>) {
    ALYRN_CHECK(engaged_, "SpawnState result was taken before completion");
    return std::move(value_);
  }

private:
  union {
    T value_;
  };
  bool engaged_{false};
};

template <>
class ResultSlot<void> {
public:
  void Take() const noexcept {}
};

template <class T>
class SpawnState {
public:
  // A SpawnState is constructed directly in kRunningJoinable. The state names
  // describe ownership and waiter progress, not coroutine frame states.
  // Keep the wait word 32-bit and naturally aligned. C++20 atomic wait is
  // portable at the API level; on Linux this representation lets libstdc++
  // wait directly on the object's futex-compatible word instead of using a
  // proxy waiter for sub-word atomics.
  enum class Phase : std::uint32_t {
    kRunningJoinable,           // running, caller attached, no joiner parked
    kWaiterParked,              // caller parked a waiter awaiting completion
    kRunningDetached,           // caller released while the root was still running
    kFinished,                  // root reached final_suspend, caller still attached
    kCompletedWaiterScheduled,  // finished and waiter ResumeWork is queued
  };

  static_assert(sizeof(Phase) == sizeof(std::uint32_t));
  static_assert(alignof(Phase) >= alignof(std::uint32_t));
  static_assert(std::atomic<Phase>::is_always_lock_free);

  enum class FinishAction : std::uint8_t {
    kKeepRoot,
    kDestroyRoot,
  };

  SpawnState() noexcept = default;

  void SetRootHandle(std::coroutine_handle<> handle) noexcept {
    ALYRN_CHECK(handle, "SpawnState requires a valid root coroutine handle");
    root_work_.SetHandle(handle);
  }

  Work* RootWork() noexcept {
    return &root_work_;
  }

  template <class U>
  void StoreResult(U&& value) {
    result_.Set(std::forward<U>(value));
  }

  // Called exactly once from SpawnRoot::final_suspend. The detached path
  // returns kDestroyRoot so the final awaiter can destroy the containing frame
  // after scheduling/completion bookkeeping is finished.
  FinishAction Finish() noexcept {
    Phase expected = Phase::kRunningJoinable;
    if (phase_.compare_exchange_strong(expected, Phase::kFinished, std::memory_order_acq_rel)) {
      phase_.notify_one();  // unblock a possible Wait(); the caller destroys later
      return FinishAction::kKeepRoot;
    }
    if (expected == Phase::kWaiterParked) {
      phase_.store(Phase::kCompletedWaiterScheduled, std::memory_order_release);

      Scheduler* scheduler = waiter_scheduler_;
      ALYRN_CHECK(scheduler != nullptr, "SpawnState parked waiter has no scheduler");
      scheduler->Schedule(&waiter_resume_);
      return FinishAction::kKeepRoot;
    }
    // expected == kRunningDetached: the caller is gone and handed the root
    // frame's final destruction to the producer.
    ALYRN_CHECK(expected == Phase::kRunningDetached,
                   "SpawnState finished from an invalid lifecycle phase");
    return FinishAction::kDestroyRoot;
  }

  // -- consumer side (the JoinHandle) -------------------------------------
  bool IsFinished() const noexcept {
    return phase_.load(std::memory_order_acquire) == Phase::kFinished;
  }

  // Async wait. Returns true when the waiter was parked (the caller suspends);
  // false when the result is already available (resume immediately).
  bool TryParkWaiter(Scheduler& scheduler, std::coroutine_handle<> waiter) noexcept {
    waiter_scheduler_ = &scheduler;
    waiter_resume_.SetHandle(waiter);
    Phase expected = Phase::kRunningJoinable;
    if (phase_.compare_exchange_strong(expected, Phase::kWaiterParked, std::memory_order_acq_rel)) {
      return true;
    }
    waiter_scheduler_ = nullptr;
    waiter_resume_.ClearHandle();
    return false;  // already kFinished
  }

  // Synchronous join: block until the root completes, then take the result.
  decltype(auto) Wait() noexcept {
    Phase phase = phase_.load(std::memory_order_acquire);
    while (phase == Phase::kRunningJoinable) {
      phase_.wait(phase, std::memory_order_acquire);
      phase = phase_.load(std::memory_order_acquire);
    }
    // JoinHandle has single-consumer semantics: Wait() and co_await cannot
    // race on the same handle. A completed async waiter has its own resume
    // path and must not also call Wait().
    ALYRN_CHECK(phase == Phase::kFinished,
                   "SpawnState::Wait raced with detach or an async joiner");
    return result_.Take();
  }

  decltype(auto) TakeResult() noexcept { return result_.Take(); }

  // The caller relinquishes the handle (Detach or destruction). Either the
  // producer takes ownership (still running) or the caller destroys the root
  // frame now (already finished). DestroyRoot() must be the final operation in
  // the latter path because this state object lives inside that frame.
  void ReleaseHandle() noexcept {
    Phase expected = Phase::kRunningJoinable;
    if (phase_.compare_exchange_strong(expected, Phase::kRunningDetached,
                                       std::memory_order_acq_rel)) {
      return;  // producer destroys the root frame in Finish()
    }
    // expected is kFinished or kCompletedWaiterScheduled: the producer is done;
    // we own and destroy the suspended root frame.
    ALYRN_CHECK(expected == Phase::kFinished || expected == Phase::kCompletedWaiterScheduled,
                   "SpawnState handle released from an invalid lifecycle phase");
    DestroyRoot();
  }

  void ClearRootHandle() noexcept { root_work_.ClearHandle(); }

private:
  void DestroyRoot() noexcept {
    auto root = root_work_.Handle();
    root_work_.ClearHandle();
    ALYRN_CHECK(root, "SpawnState has no root coroutine to destroy");
    root.destroy();
  }

  ResumeWork root_work_;
  // ResultSlot<void> is empty; let it overlap the root Work instead of
  // introducing an alignment-only hole in every void root frame.
  [[no_unique_address]] ResultSlot<T> result_;
  Scheduler* waiter_scheduler_{nullptr};
  ResumeWork waiter_resume_;
  std::atomic<Phase> phase_{Phase::kRunningJoinable};
};

}  // namespace alyrn::coro::detail
