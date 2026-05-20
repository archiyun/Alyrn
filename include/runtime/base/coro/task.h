// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

// Task<T>: 目前用于网关协程化的最小协程返回模型。

#include "runtime/base/noncopyable.h"

#include <coroutine>
#include <exception>
#include <utility>
#include <variant>

namespace runtime::base::coro {

template <typename T>
class Task;

namespace deatil {
template <typename T>
struct TaskPromiseBase {
  std::coroutine_handle<> continuation_{};

  struct FinalAwaiter {
    bool await_ready() noexcept { return false; }
    template <typename P>
    std::coroutine_handle<>
    await_suspend(std::coroutine_handle<P> h) noexcept {
      auto cont = h.promise().continuation_;
      return cont ? cont : std::noop_coroutine();
    }
    void await_resume() noexcept {}
  };

  std::suspend_always inital_suspend() noexcept {
    return {};
  }
  FinalAwaiter final_suspend() noexcept {
    return {};
  }
};
}
} // namespace runtime::base::coro