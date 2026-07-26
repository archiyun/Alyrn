// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: the atomic lifecycle state shared by a
// spawned coroutine (the producer) and its JoinHandle (the consumer). It
// resolves the race between task completion, waiting, and detach, and decides
// which side frees the heap state. It owns the driver Work and the scheduler
// selected by the parent to re-submit a parked waiter. The scheduler used to
// start the driver is supplied by Spawn and need not resume the waiter.
#pragma once

#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <new>
#include <semaphore>
#include <type_traits>
#include <utility>

#include "coropact/coro/scheduler.h"
#include "coropact/coro/detail/spawn_stats.h"
#include "coropact/coro/work.h"

namespace coropact::coro::detail {

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
    assert(!engaged_);
    std::construct_at(std::addressof(value_), std::forward<U>(value));
    engaged_ = true;
  }

  T Take() noexcept(std::is_nothrow_move_constructible_v<T>) {
    assert(engaged_);
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
  enum class Phase : std::uint8_t {
    kRunningJoinable,  // running, caller attached, no joiner parked
    kWaiterParked,      // caller parked a waiter awaiting completion
    kRunningDetached,  // caller released while the root was still running
    kFinished,         // driver finished, caller still attached
    kCompletedWaiterScheduled,  // finished and waiter ResumeWork is queued
  };

  SpawnState() noexcept = default;

#if defined(COROPACT_ENABLE_SPAWN_STATS)
  ~SpawnState() noexcept { RecordSpawnStateDeallocation(sizeof(SpawnState)); }
#endif

  // -- driver scheduling --------------------------------------------------
  void set_driver_handle(std::coroutine_handle<> handle) noexcept {
    driver_work_.SetHandle(handle);
  }

  [[nodiscard]]
  Work* driver_work() noexcept {
    return &driver_work_;
  }

  // -- producer side (the spawned driver coroutine) -----------------------
  template <class U>
  void StoreResult(U&& value) {
    result_.Set(std::forward<U>(value));
  }

  // Called exactly once when the driver completes. May delete *this in the
  // detached case; the caller must not touch the state afterwards.
  void Finish() noexcept {
    Phase expected = Phase::kRunningJoinable;
    if (phase_.compare_exchange_strong(expected, Phase::kFinished, std::memory_order_acq_rel)) {
      ready_.release();  // unblock a possible Wait(); the caller frees later
      return;
    }
    if (expected == Phase::kWaiterParked) {
      phase_.store(Phase::kCompletedWaiterScheduled, std::memory_order_release);
      ready_.release();

      Scheduler* scheduler = waiter_scheduler_;
      assert(scheduler != nullptr);
      scheduler->Schedule(&waiter_resume_);
      return;
    }
    // expected == kRunningDetached: the caller is gone and handed us ownership.
    delete this;
  }

  // -- consumer side (the JoinHandle) -------------------------------------
  [[nodiscard]]
  bool IsFinished() const noexcept {
    return phase_.load(std::memory_order_acquire) == Phase::kFinished;
  }

  // Async wait. Returns true when the waiter was parked (the caller suspends);
  // false when the result is already available (resume immediately).
  [[nodiscard]]
  bool TryParkWaiter(Scheduler& scheduler, std::coroutine_handle<> waiter) noexcept {
    waiter_scheduler_ = &scheduler;
    waiter_resume_.SetHandle(waiter);
    Phase expected = Phase::kRunningJoinable;
    if (phase_.compare_exchange_strong(expected, Phase::kWaiterParked,
                                       std::memory_order_acq_rel)) {
      return true;
    }
    waiter_scheduler_ = nullptr;
    waiter_resume_.ClearHandle();
    return false;  // already kFinished
  }

  // Synchronous join: block until the root completes, then take the result.
  decltype(auto) Wait() noexcept {
    ready_.acquire();
    return result_.Take();
  }

  decltype(auto) TakeResult() noexcept { return result_.Take(); }

  // The caller relinquishes the handle (Detach or destruction). Either the
  // producer takes ownership (still running) or we free now (already finished).
  void ReleaseHandle() noexcept {
    Phase expected = Phase::kRunningJoinable;
    if (phase_.compare_exchange_strong(expected, Phase::kRunningDetached,
                                       std::memory_order_acq_rel)) {
      return;  // producer deletes on Finish()
    }
    // expected is kFinished or kCompletedWaiterScheduled: the producer is done;
    // we own and free.
    delete this;
  }

private:
  ResultSlot<T> result_;
  ResumeWork driver_work_;
  Scheduler* waiter_scheduler_{nullptr};
  ResumeWork waiter_resume_;
  std::atomic<Phase> phase_{Phase::kRunningJoinable};
  std::binary_semaphore ready_{0};
};

}  // namespace coropact::coro::detail
