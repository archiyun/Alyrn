// SPDX-License-Identifier: MIT

#pragma once

#include <coroutine>
#include <exception>
#include <utility>

#include "alyrn/coro/frame_allocator.h"
#include "alyrn/coro/work.h"
#include "alyrn/detail/check.h"
#include "alyrn/detail/macros.h"

namespace alyrn::coro {

class [[nodiscard]] DetachedTask {
public:
  ALYRN_DELETE_COPY(DetachedTask);

  using CompletionFn = void (*)(void* context) noexcept;

  struct promise_type : public detail::FrameAllocationSupport, public ResumeWork {
    DetachedTask get_return_object() noexcept {
      return DetachedTask{std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    auto initial_suspend() const noexcept { return std::suspend_always{}; }
    auto final_suspend() const noexcept {
      if (on_complete != nullptr) {
        on_complete(on_complete_context);
      }
      return std::suspend_never{};
    }

    void return_void() const noexcept {}
    void unhandled_exception() const noexcept { std::terminate(); }

    CompletionFn on_complete{nullptr};
    void* on_complete_context{nullptr};
  };

  using Handle = std::coroutine_handle<promise_type>;

  explicit DetachedTask(Handle handle) noexcept : handle_(handle) {}
  ~DetachedTask() noexcept {
    if (handle_) {
      handle_.destroy();
    }
  }

  DetachedTask(DetachedTask&& other) noexcept : handle_(other.Release()) {}
  DetachedTask& operator=(DetachedTask&& other) noexcept {
    if (this == &other) {
      return *this;
    }

    if (handle_) {
      handle_.destroy();
    }
    handle_ = other.Release();
    return *this;
  }

  Handle Release() noexcept { return std::exchange(handle_, {}); }

  // Calls fn(context) once when the coroutine finishes, on the thread that
  // ran it, just before its frame is destroyed; its parameters, such as an
  // owned Stream, are still alive then. For supervisors that count running
  // tasks, like a server draining its connections. Set it before the task is
  // spawned.
  void OnComplete(CompletionFn fn, void* context) noexcept {
    ALYRN_CHECK(handle_, "DetachedTask::OnComplete requires a task that was not released");
    handle_.promise().on_complete = fn;
    handle_.promise().on_complete_context = context;
  }

private:
  Handle handle_;
};

}  // namespace alyrn::coro
