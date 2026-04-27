#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/task/scheduler_metrics.h"
#include "runtime/task/task_state.h"
#include "runtime/task/work_queue.h"

#include <cstddef>
#include <thread>
#include <vector>

namespace runtime::task {

// ThreadPool pulls Tasks from WorkQueue and executes them.
// It owns no queue and holds no task identity; it only drives execution.
// queue and metrics must outlive this ThreadPool.
class ThreadPool : public runtime::base::NonCopyable {
 public:
  ThreadPool(WorkQueue& queue, SchedulerMetrics& metrics,
             std::size_t thread_count = 0);
  ~ThreadPool();

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
  SchedulerMetrics& metrics_;
  std::vector<std::jthread> workers_;
};

}  // namespace runtime::task
