// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <queue>
#include <stop_token>

#include "runtime/task/detail/task.h"

namespace runtime::task {

// Thread-safe bounded FIFO queue for Tasks.
class WorkQueue {
 public:
  enum class PushStatus {
    kOk,
    kFull,
    kShutdown,
  };

  // max_size == 0 means unbounded.
  explicit WorkQueue(std::size_t max_size = 0);

  // Returns the precise reason a task was accepted or rejected.
  PushStatus TryPush(std::shared_ptr<Task>&& task);

  // Returns false if the queue has been shut down.
  bool Push(std::shared_ptr<Task>&& task);

  // Blocks until a task is available or stoken fires.
  // Returns nullptr when the worker should exit.
  std::shared_ptr<Task> Wait(std::stop_token stoken);

  // Non-blocking pop. Returns nullptr if empty.
  std::shared_ptr<Task> TryPop();

  std::size_t size() const;

  // Wakes all blocked Wait() calls so workers can drain and exit.
  void Shutdown();

 private:
  std::size_t max_size_{0};
  mutable std::mutex          mutex_;
  std::condition_variable_any cv_;
  std::queue<std::shared_ptr<Task>> queue_;
  bool shutdown_{false};
};

}  // namespace runtime::task
