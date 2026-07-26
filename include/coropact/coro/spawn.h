// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: Spawn(Scheduler&, Task<T>) -> JoinHandle<T> and
// SpawnDetach(Scheduler&, Task<T>) -> void. Spawn uses an internal driver
// coroutine to publish its result to a SpawnState; SpawnDetach uses a smaller
// self-destroying driver with no join state. The Task itself is unaware it was
// spawned.
#pragma once

#include <coroutine>
#include <exception>
#include <type_traits>
#include <utility>

#include "coropact/coro/detail/spawn_state.h"
#include "coropact/coro/frame_allocator.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/task.h"
#include "coropact/coro/work.h"
#include "coropact/utils/macros.h"

namespace coropact::coro {

template <Returnable T>
class [[nodiscard]] JoinHandle {
public:
  COROPACT_DELETE_COPY(JoinHandle);

  using State = detail::SpawnState<T>;

  explicit JoinHandle(State* state) noexcept : state_(state) {}
  JoinHandle(JoinHandle&& other) noexcept : state_(std::exchange(other.state_, nullptr)) {}
  JoinHandle& operator=(JoinHandle&& other) noexcept {
    if (this != &other) {
      Reset();
      state_ = std::exchange(other.state_, nullptr);
    }
    return *this;
  }
  ~JoinHandle() { Reset(); }

  // Synchronous join: blocks the calling thread until completion. Never call
  // this on an IO loop thread -- it would block the loop.
  decltype(auto) Wait() noexcept { return state_->Wait(); }

  // Detach: give up the result; the coroutine still runs to completion and
  // cleans itself up.
  void Detach() noexcept { Reset(); }

  // Async join from inside another coroutine.
  auto operator co_await() && noexcept {
    struct Awaiter {
      State* state;

      [[nodiscard]]
      bool await_ready() const noexcept {
        return state->IsFinished();
      }
      [[nodiscard]]
      bool await_suspend(std::coroutine_handle<> joiner) noexcept {
        return state->TryParkWaiter(Scheduler::RequireCurrent(), joiner);
      }
      decltype(auto) await_resume() noexcept { return state->TakeResult(); }
    };
    return Awaiter{state_};
  }

private:
  void Reset() noexcept {
    if (state_) {
      state_->ReleaseHandle();
      state_ = nullptr;
    }
  }

  State* state_;
};

namespace detail {

// Self-destroying driver frame: scheduled once, resumes the leaf Task, stores
// the result, then signals the SpawnState. final_suspend = suspend_never frees
// it.
struct SpawnDriver {
  struct promise_type : FrameAllocationSupport {
    SpawnDriver get_return_object() noexcept {
      return SpawnDriver{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    auto initial_suspend() const noexcept { return std::suspend_always{}; }
    auto final_suspend() const noexcept { return std::suspend_never{}; }
    void return_void() const noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };

  std::coroutine_handle<promise_type> handle;
};

template <Returnable T>
SpawnDriver RunSpawn(SpawnState<T>* state, Task<T> task) {
  if constexpr (std::is_void_v<T>) {
    co_await std::move(task);
  } else {
    state->StoreResult(co_await std::move(task));
  }
  state->Finish();  // may delete state; do not touch it afterwards
}

// Self-destroying fire-and-forget driver. The ResumeWork lives in the promise
// frame because Scheduler stores a non-owning Work* until the work is run.
struct DetachedDriver {
  struct promise_type : FrameAllocationSupport {
    ResumeWork driver_work;

    DetachedDriver get_return_object() noexcept {
      return DetachedDriver{std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    auto initial_suspend() const noexcept { return std::suspend_always{}; }
    auto final_suspend() const noexcept { return std::suspend_never{}; }
    void return_void() const noexcept {}
    void unhandled_exception() const noexcept { std::terminate(); }
  };

  std::coroutine_handle<promise_type> handle;
};

template <Returnable T>
DetachedDriver RunDetached(Task<T> task) {
  // A detached caller has no result consumer. Task<T> is still supported for
  // convenience, but Task<void> avoids constructing and moving a discarded T.
  if constexpr (std::is_void_v<T>) {
    co_await std::move(task);
  } else {
    static_cast<void>(co_await std::move(task));
  }
}

}  // namespace detail

template <Returnable T>
[[nodiscard]]
JoinHandle<T> Spawn(Scheduler& scheduler, Task<T> task) {
  auto* state = new detail::SpawnState<T>();
  detail::SpawnDriver driver = detail::RunSpawn<T>(state, std::move(task));
  state->set_driver_handle(driver.handle);
  scheduler.Schedule(state->driver_work());
  return JoinHandle<T>{state};
}

// Schedules task without creating SpawnState or returning a JoinHandle. The
// task continues until completion, but cannot be joined or cancelled through
// this API.
template <Returnable T>
void SpawnDetach(Scheduler& scheduler, Task<T> task) {
  auto driver = detail::RunDetached(std::move(task));

  auto handle = driver.handle;
  auto& promise = handle.promise();

  promise.driver_work.handle = handle;
  scheduler.Schedule(&promise.driver_work);
}

}  // namespace coropact::coro
