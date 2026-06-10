// Copyright (c) RomenJens
// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>

namespace runtime::coro {

template <class T>
class Task;

namespace detail {

class TaskPromiseBase {
public:
  std::suspend_always initial_suspend() noexcept { return {}; }

  struct FinalAwaiter {
    bool await_ready() const noexcept { return false; }

    template <class P>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<P> self) noexcept {
      auto cont = self.promise().continuation();
      return cont ? cont : std::noop_coroutine();
    }

    void await_resume() const noexcept {}
  };
  FinalAwaiter final_suspend() noexcept { return {}; }

  void unhandled_exception() noexcept { exception_ = std::current_exception(); }
  void set_continuation(std::coroutine_handle<> c) noexcept { continuation_ = c; }

protected:
  void RethrowIfException() const {
    if (exception_) std::rethrow_exception(exception_);
  }
  std::coroutine_handle<> continuation_{};
  std::exception_ptr exception_{};
};

template <class T>
class TaskPromise final : public TaskPromiseBase {
public:
  Task<T> get_return_object() noexcept;
  void return_void() noexcept {}
  void result() { RethrowIfException(); }
};

template <class T = void>
class Task {
public:
  using promise_type = detail::TaskPromise<T>;
  using Handle = std::coroutine_handle<promise_type>;

  Task() noexcept = default;
  explicit Task(Handle h) noexcept : coro_(h) {}

  Task(Task&& other) noexcept : coro_(std::exchange(other.coro_, {})) {}
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      if (coro_) coro_.destroy();
      coro_ = std::exchange(other.coro_, {});
    }
    return *this;
  }
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  ~Task() {
    if (coro_) coro_.destroy();
  }
  struct Awaiter {
    Handle callee;

    bool await_ready() { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
      callee.promise().set_continuation(caller);
      return callee;
    }

    T await_resume() { return callee.promise().result(); }
  };
  Awaiter operator co_await() const noexcept { return Awaiter{coro_}; }

  bool done() const noexcept { return !coro || }

private:
  Handle coro_;
};

}  // namespace detail

}  // namespace runtime::coro
