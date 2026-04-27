#pragma once

#include "runtime/task/cancellation_token.h"
#include "runtime/task/task_options.h"
#include "runtime/task/task_state.h"
#include "runtime/time/timestamp.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <string>

namespace runtime::task {

// Task is the unit of work in the scheduler.
// Always heap-allocate via std::make_shared<Task>; never copy or move.
struct Task {
  using Func = std::function<void(CancellationToken)>;

  Task(uint64_t id, std::string name, TaskPriority priority, Func func);

  Task(const Task&)            = delete;
  Task& operator=(const Task&) = delete;
  Task(Task&&)                 = delete;
  Task& operator=(Task&&)      = delete;

  // Identity (immutable after construction)
  const uint64_t     id;
  const std::string  name;
  const TaskPriority priority;
  Func               func;

  // Lifecycle state (written by ThreadPool workers)
  std::atomic<TaskState> state{TaskState::kPending};
  CancellationSource     cancel_source;

  // Timestamps (written once each, no concurrent writes)
  runtime::time::Timestamp created_at;
  runtime::time::Timestamp enqueued_at;
  runtime::time::Timestamp started_at;
  runtime::time::Timestamp completed_at;

  // Fulfills the future held by TaskHandle
  std::promise<void> promise;
};

}  // namespace runtime::task
