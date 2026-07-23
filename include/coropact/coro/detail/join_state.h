// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: the atomic join/detach state machine shared by a
// spawned coroutine (the producer) and its JoinHandle (the consumer). It
// resolves the three-way race between "coroutine finished", "caller joined", and
// "caller detached", and decides which side frees the heap state. It holds a
// parked continuation handle and the initial ResumeWork, but knows nothing about
// schedulers or IO beyond that.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <new>
#include <semaphore>
#include <utility>

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
    ::new (static_cast<void*>(std::addressof(value_))) T(std::forward<U>(value));
    engaged_ = true;
  }
  T Take() noexcept { return std::move(value_); }

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
class JoinState {
public:
  // The six phases mirror condy's model. kIdle is the conceptual pre-scheduling
  // phase; it collapses into kRunningJoinable at construction because the root
  // is logically joinable the instant the state exists.
  enum class Phase : std::uint8_t {
    kIdle,
    kRunningJoinable,  // running, caller attached, no joiner parked
    kRunningJoining,   // caller parked a joiner awaiting completion
    kRunningDetached,  // caller released while the root was still running
    kFinished,         // root finished, caller still attached
    kZombie,           // finished and a parked joiner was already resumed
  };

  JoinState() noexcept = default;

  // -- initial scheduling -------------------------------------------------
  void set_start_handle(std::coroutine_handle<> handle) noexcept { start_work_.handle = handle; }
  Work* start_work() noexcept { return &start_work_; }

  // -- producer side (the spawned root coroutine) -------------------------
  template <class U>
  void SetResult(U&& value) {
    result_.Set(std::forward<U>(value));
  }

  // Called exactly once when the root completes. May delete *this in the
  // detached case; the caller must not touch the state afterwards.
  void Complete() noexcept {
    Phase expected = Phase::kRunningJoinable;
    if (phase_.compare_exchange_strong(expected, Phase::kFinished, std::memory_order_acq_rel)) {
      ready_.release();  // unblock a possible Wait(); the caller frees later
      return;
    }
    if (expected == Phase::kRunningJoining) {
      std::coroutine_handle<> joiner = joiner_;
      phase_.store(Phase::kZombie, std::memory_order_release);
      ready_.release();
      joiner.resume();  // the caller's JoinHandle frees the state after this
      return;
    }
    // expected == kRunningDetached: the caller is gone and handed us ownership.
    delete this;
  }

  // -- consumer side (the JoinHandle) -------------------------------------
  bool IsFinished() const noexcept {
    return phase_.load(std::memory_order_acquire) == Phase::kFinished;
  }

  // Async join. Returns true when the joiner was parked (the caller suspends);
  // false when the result is already available (resume immediately).
  bool TryParkJoiner(std::coroutine_handle<> joiner) noexcept {
    joiner_ = joiner;
    Phase expected = Phase::kRunningJoinable;
    if (phase_.compare_exchange_strong(expected, Phase::kRunningJoining,
                                       std::memory_order_acq_rel)) {
      return true;
    }
    joiner_ = {};
    return false;  // already kFinished
  }

  // Synchronous join: block until the root completes, then take the result.
  decltype(auto) Wait() noexcept {
    ready_.acquire();
    return result_.Take();
  }

  decltype(auto) TakeResult() noexcept { return result_.Take(); }

  // The caller relinquishes the state (Detach or destruction). Either the
  // producer takes ownership (still running) or we free now (already finished).
  void ConsumerRelease() noexcept {
    Phase expected = Phase::kRunningJoinable;
    if (phase_.compare_exchange_strong(expected, Phase::kRunningDetached,
                                       std::memory_order_acq_rel)) {
      return;  // producer deletes on Complete()
    }
    // expected is kFinished or kZombie: the producer is done; we own and free.
    delete this;
  }

private:
  ResultSlot<T> result_;
  ResumeWork start_work_{};
  std::atomic<Phase> phase_{Phase::kRunningJoinable};
  std::coroutine_handle<> joiner_{};
  std::binary_semaphore ready_{0};
};

}  // namespace coropact::coro::detail
