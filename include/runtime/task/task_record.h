#pragma once

#include "runtime/task/task_options.h"
#include "runtime/task/task_state.h"
#include "runtime/time/timestamp.h"

#include <cstdint>
#include <string>

namespace runtime::task {

struct Task;

// Immutable snapshot of a Task taken when it reaches a terminal state.
// Copyable and movable — safe to store outside the task's lifetime.
struct TaskRecord {
  uint64_t    id;
  std::string name;
  TaskPriority priority;
  TaskState    state;

  runtime::time::Timestamp created_at;
  runtime::time::Timestamp enqueued_at;
  runtime::time::Timestamp started_at;
  runtime::time::Timestamp completed_at;

  static TaskRecord From(const Task& task);
};

}  // namespace runtime::task
