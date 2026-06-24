// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <thread>
#include <vector>

#include "vexo/task/executor_metrics.h"
#include "vexo/task/task_state.h"
#include "vexo/task/detail/work_queue.h"
#include "vexo/utils/macros.h"

namespace vexo::task {

// ThreadPool pulls Tasks from WorkQueue and executes them.
// It owns no queue and holds no task identity; it only drives execution.
// queue and metrics must outlive this ThreadPool.
class ThreadPool {
 public:
  ThreadPool(WorkQueue& queue, ExecutorMetrics& metrics,
             std::size_t thread_count = 0);
  ~ThreadPool();

  VEXO_DELETE_COPY_MOVE(ThreadPool);

 private:
  void WorkerLoop(std::stop_token stoken);

  // Resolves task promise with set_value() and updates terminal counters.
  // Caller must decrement running_count before calling.
  void CompleteTask(Task& task, TaskState final_state);

  // Resolves task promise with set_exception() for the kFailed case.
  // Must be called from inside a catch block (uses std::current_exception()).
  // Caller must decrement running_count before calling.
  void FailTask(Task& task);

  WorkQueue&        queue_;
  ExecutorMetrics& metrics_;
  std::vector<std::jthread> workers_;
};

}  // namespace vexo::task
