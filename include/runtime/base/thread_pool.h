// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

// Lightweight fire-and-forget thread pool for offloading blocking operations.
//
// Intended use: tasks that block the calling thread (file I/O, DNS lookups,
// synchronous syscalls) and must not stall an event-loop IO thread. The caller
// submits a lambda and returns immediately; a worker thread executes it later.
//
// NOT intended for:
//   - Tasks that need futures/promises or result propagation — use the
//     runtime::task scheduler instead (includes WorkQueue + TaskHistory).
//   - CPU-bound parallel work that needs fine-grained scheduling.
//   - Tasks that require guaranteed ordering between submissions.
//
// Thread safety: Submit() and Stop() are safe to call from any thread.
//
// Requires C++20 (std::jthread, std::stop_token, condition_variable_any).

#include "runtime/base/noncopyable.h"

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace runtime::base {

class ThreadPool : public NonCopyable {
public:
  // thread_count == 0: use hardware_concurrency(), fallback to 4.
  explicit ThreadPool(std::size_t thread_count = 0) {
    if (thread_count == 0) thread_count = std::thread::hardware_concurrency();
    if (thread_count == 0) thread_count = 4;
    workers_.reserve(thread_count);
    for (std::size_t i = 0; i < thread_count; ++i) {
      workers_.emplace_back([this](std::stop_token stoken) {
        WorkerLoop(stoken);
      });
    }
  }

  // Drains the pool and joins all worker threads.
  ~ThreadPool() {
    Stop();
  }

  // Enqueues a task for execution. Returns false if the pool is stopped.
  // The caller must not rely on execution order relative to other submissions.
  bool Submit(std::function<void()> task) {
    {
      std::lock_guard<std::mutex> lk(mutex_);
      if (stop_) return false;
      queue_.push(std::move(task));
    }
    cv_.notify_one();
    return true;
  }

  // Signals all workers to stop after draining pending tasks, then joins them.
  // Idempotent: safe to call multiple times.
  void Stop() {
    {
      std::lock_guard<std::mutex> lk(mutex_);
      if (stop_) return;
      stop_ = true;
    }
    cv_.notify_all();
    workers_.clear();  // jthread destructor joins each thread
  }

private:
  void WorkerLoop(std::stop_token stoken) {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lk(mutex_);
        // condition_variable_any supports stop_token natively (C++20),
        // so an external stop_requested() wakeup requires no extra predicate.
        cv_.wait(lk, stoken, [this] { return stop_ || !queue_.empty(); });

        if (queue_.empty()) return;  // woken by stop signal with empty queue
        task = std::move(queue_.front());
        queue_.pop();
      }

      try {
        task();
      } catch (...) {
        // Swallow exceptions to keep the worker alive. Tasks that need error
        // propagation should capture a promise or invoke a callback instead.
      }
    }
  }

private:
  mutable std::mutex              mutex_;
  std::condition_variable_any     cv_;   // _any required for stop_token overload
  std::queue<std::function<void()>> queue_;
  std::vector<std::jthread>       workers_;
  bool                            stop_{false};
};

} // namespace runtime::base