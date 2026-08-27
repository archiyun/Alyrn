// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>
#include <coroutine>
#include <exception>
#include <type_traits>
#include <utility>

#include "alyrn/coro/detail/spawn_state.h"
#include "alyrn/coro/frame_allocator.h"
#include "alyrn/coro/task.h"

namespace alyrn::coro::detail {

template <Returnable T>
struct SpawnRootPromise;

// Internal joinable root owner. Its promise frame contains both the join
// state and the root ResumeWork, so Spawn no longer needs a separate
// SpawnState allocation. The business Task remains a separate child frame.
template <Returnable T>
struct SpawnRoot {
  ALYRN_DELETE_COPY(SpawnRoot);

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
  bool await_ready() const noexcept { return false; }

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

}  // namespace alyrn::coro::detail
