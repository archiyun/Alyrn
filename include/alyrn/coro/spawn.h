// SPDX-License-Identifier: MIT
//
// Spawn(Scheduler&, Task<T>) produces a joinable root; SpawnDetach schedules a
// detached root. Root-frame, final-suspend, and result-lifetime machinery is
// intentionally kept in coro/detail/spawn_root.h.
#pragma once

#include <coroutine>
#include <utility>

#include "alyrn/detail/check.h"
#include "alyrn/coro/detached_task.h"
#include "alyrn/coro/detail/spawn_state.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/task.h"
#include "alyrn/coro/work.h"
#include "alyrn/detail/macros.h"

namespace alyrn::coro {

template <Returnable T>
class [[nodiscard]] JoinHandle {
public:
  ALYRN_DELETE_COPY(JoinHandle);

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
  decltype(auto) Wait() noexcept {
    ALYRN_CHECK(state_ != nullptr, "JoinHandle::Wait called after ownership was released");
    return state_->Wait();
  }

  // Detach: give up the result; the coroutine still runs to completion and
  // cleans itself up.
  void Detach() noexcept { Reset(); }

  // Async join from inside another coroutine.
  auto operator co_await() && noexcept {
    ALYRN_CHECK(state_ != nullptr,
                   "JoinHandle::operator co_await called after ownership was released");
    struct Awaiter {
      State* state;

      bool await_ready() const noexcept { return state->IsFinished(); }
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

}  // namespace alyrn::coro

#include "alyrn/coro/detail/spawn_root.h"

namespace alyrn::coro {

template <Returnable T>
JoinHandle<T> Spawn(Scheduler& scheduler, Task<T> task) {
  auto child = std::move(task).operator co_await();
  auto root = detail::RunSpawn<T>(std::move(child));
  auto handle = root.Release();
  ALYRN_CHECK(handle, "Spawn failed to create a root coroutine");

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
  ALYRN_CHECK(handle, "SpawnDetach requires a non-empty DetachedTask");
  auto& work = static_cast<ResumeWork&>(handle.promise());
  work.SetHandle(handle);
  scheduler.Schedule(&work);
}

}  // namespace alyrn::coro
