// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#pragma once

#include <coroutine>
#include <exception>
#include <utility>

#include "coropact/coro/frame_allocator.h"
#include "coropact/coro/work.h"
#include "coropact/utils/macros.h"

namespace coropact::coro {

class [[nodiscard]] DetachedTask {
public:
  COROPACT_DELETE_COPY(DetachedTask);

  struct promise_type : public detail::FrameAllocationSupport, public ResumeWork {
    DetachedTask get_return_object() noexcept {
      return DetachedTask{std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    auto initial_suspend() const noexcept { return std::suspend_always{}; }
    auto final_suspend() const noexcept { return std::suspend_never{}; }

    void return_void() const noexcept {}
    void unhandled_exception() const noexcept { std::terminate(); }
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

private:
  Handle handle_;
};

}  // namespace coropact::coro
