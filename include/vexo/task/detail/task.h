// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>

#include "vexo/task/cancellation_token.h"
#include "vexo/task/task_state.h"

namespace vexo::task {

// Task is the unit of work in the scheduler.
// Always heap-allocate via std::make_shared<Task>; never copy or move.
struct Task {
  using Func = std::function<void(CancellationToken)>;

  Task(uint64_t id, Func func);

  Task(const Task&)            = delete;
  Task& operator=(const Task&) = delete;
  Task(Task&&)                 = delete;
  Task& operator=(Task&&)      = delete;

  // Identity (immutable after construction)
  const uint64_t id;
  Func func;

  // Lifecycle state (written by ThreadPool workers)
  std::atomic<TaskState> state{TaskState::kPending};
  CancellationSource     cancel_source;

  // Fulfills the future held by TaskHandle
  std::promise<void> promise;
};

}  // namespace vexo::task
