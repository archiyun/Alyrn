// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: Spawn(Scheduler&, Task<T>) -> JoinHandle<T> and
// fire-and-forget SpawnDetach(Scheduler&, DetachedTask). The detached root
// embeds ResumeWork in the business coroutine frame and owns no join state.
// Task itself remains the lazy, result-producing child coroutine used by Spawn
// and co_await composition.
#pragma once

#include <cassert>
#include <coroutine>
#include <exception>
#include <type_traits>
#include <utility>

#include "coropact/coro/detached_task.h"
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

template <Returnable T>
struct SpawnRootPromise;

// Internal joinable root owner. Its promise frame contains both the join
// state and the root ResumeWork, so Spawn no longer needs a separate
// SpawnState allocation. The business Task remains a separate child frame.
template <Returnable T>
struct SpawnRoot {
  COROPACT_DELETE_COPY(SpawnRoot);

  using promise_type = SpawnRootPromise<T>;
  using Handle = std::coroutine_handle<promise_type>;

  explicit SpawnRoot(Handle handle) noexcept : handle_(handle) {}

  SpawnRoot(SpawnRoot&& other) noexcept : handle_(other.Release()) {}
  SpawnRoot& operator=(SpawnRoot&& other) noexcept {
    if (this != &other) {
      if (handle_) {
        handle_.destroy();
      }
      handle_ = other.Release();
    }
    return *this;
  }

  ~SpawnRoot() {
    if (handle_) {
      handle_.destroy();
    }
  }

  Handle Release() noexcept { return std::exchange(handle_, {}); }

private:
  Handle handle_{};
};

template <Returnable T>
struct SpawnFinalAwaiter {
  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  template <class Promise>
  [[nodiscard]]
  std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> handle) noexcept {
    auto& state = static_cast<SpawnState<T>&>(handle.promise());
    if (state.Finish() == SpawnState<T>::FinishAction::kDestroyRoot) {
      state.ClearRootHandle();
      handle.destroy();
    }
    return std::noop_coroutine();
  }

  void await_resume() const noexcept {}
};

template <Returnable T, class Promise>
struct SpawnRootPromiseBase : FrameAllocationSupport, SpawnState<T> {
  SpawnRoot<T> get_return_object() noexcept;

  auto initial_suspend() const noexcept { return std::suspend_always{}; }
  auto final_suspend() noexcept { return SpawnFinalAwaiter<T>{}; }
  void unhandled_exception() noexcept { std::terminate(); }
};

template <Returnable T>
struct SpawnRootPromise : SpawnRootPromiseBase<T, SpawnRootPromise<T>> {
  void return_value(T value) noexcept(std::is_nothrow_move_constructible_v<T>) {
    this->StoreResult(std::move(value));
  }
};

template <>
struct SpawnRootPromise<void> : SpawnRootPromiseBase<void, SpawnRootPromise<void>> {
  void return_void() const noexcept {}
};

template <Returnable T, class Promise>
SpawnRoot<T> SpawnRootPromiseBase<T, Promise>::get_return_object() noexcept {
  return SpawnRoot<T>{std::coroutine_handle<Promise>::from_promise(static_cast<Promise&>(*this))};
}

// Accept the already-created child awaiter so the root frame stores one child
// handle instead of retaining both a Task wrapper and its awaiter.
template <Returnable T>
SpawnRoot<T> RunSpawn(TaskAwaiter<T> child) {
  if constexpr (std::is_void_v<T>) {
    co_await child;
    co_return;
  } else {
    co_return co_await child;
  }
}

}  // namespace detail

template <Returnable T>
[[nodiscard]]
JoinHandle<T> Spawn(Scheduler& scheduler, Task<T> task) {
  auto child = std::move(task).operator co_await();
  auto root = detail::RunSpawn<T>(std::move(child));
  auto handle = root.Release();
  assert(handle);

  auto& promise = handle.promise();
  auto& state = static_cast<detail::SpawnState<T>&>(promise);
  state.SetRootHandle(handle);

  scheduler.Schedule(state.RootWork());
  return JoinHandle<T>{&state};
}

// Schedules a fire-and-forget root. The task continues until completion, but
// cannot be joined or cancelled through this API.
inline void SpawnDetach(Scheduler& scheduler, DetachedTask task) noexcept {
  auto handle = task.Release();
  assert(handle && "SpawnDetach requires a non-empty DetachedTask");
  auto& work = static_cast<ResumeWork&>(handle.promise());
  work.SetHandle(handle);
  scheduler.Schedule(&work);
}

}  // namespace coropact::coro
