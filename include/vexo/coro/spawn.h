// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Single responsibility: Spawn(Scheduler&, Task<T>) -> JoinHandle<T>. An internal
// root coroutine runs the leaf Task on the scheduler and publishes its result to
// a JoinState. JoinHandle exposes async co_await, synchronous Wait(), and
// Detach(). This is the only place that touches both JoinState and Scheduler;
// the Task itself is unaware it was spawned.
#pragma once

#include <coroutine>
#include <exception>
#include <type_traits>
#include <utility>

#include "vexo/coro/detail/join_state.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/task.h"
#include "vexo/coro/work.h"
#include "vexo/utils/macros.h"

namespace vexo::coro {

template <Returnable T>
class [[nodiscard]] JoinHandle {
public:
  VEXO_DELETE_COPY(JoinHandle);
  using State = detail::JoinState<T>;

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
      bool await_ready() const noexcept { return state->IsFinished(); }
      bool await_suspend(std::coroutine_handle<> joiner) noexcept {
        return state->TryParkJoiner(joiner);
      }
      decltype(auto) await_resume() noexcept { return state->TakeResult(); }
    };
    return Awaiter{state_};
  }

private:
  void Reset() noexcept {
    if (state_) {
      state_->ConsumerRelease();
      state_ = nullptr;
    }
  }

  State* state_;
};

namespace detail {

// Self-destroying root frame: scheduled once, resumes the leaf Task, stores the
// result, then signals the JoinState. final_suspend = suspend_never frees it.
struct SpawnRoot {
  struct promise_type {
    SpawnRoot get_return_object() noexcept {
      return SpawnRoot{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_always initial_suspend() const noexcept { return {}; }
    std::suspend_never final_suspend() const noexcept { return {}; }
    void return_void() const noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };

  std::coroutine_handle<promise_type> handle;
};

template <Returnable T>
SpawnRoot RunSpawn(JoinState<T>* state, Task<T> task) {
  if constexpr (std::is_void_v<T>) {
    co_await std::move(task);
  } else {
    state->SetResult(co_await std::move(task));
  }
  state->Complete();  // may delete state; do not touch it afterwards
}

}  // namespace detail

template <Returnable T>
[[nodiscard]] JoinHandle<T> Spawn(Scheduler& scheduler, Task<T> task) {
  auto* state = new detail::JoinState<T>();
  detail::SpawnRoot root = detail::RunSpawn<T>(state, std::move(task));
  state->set_start_handle(root.handle);
  scheduler.Schedule(state->start_work());
  return JoinHandle<T>{state};
}

}  // namespace vexo::coro
