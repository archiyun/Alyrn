#include "runtime/task/thread_pool.h"

#include "runtime/task/task.h"
#include "runtime/time/timestamp.h"

namespace runtime::task {

ThreadPool::ThreadPool(WorkQueue& queue, SchedulerMetrics& metrics,
                       std::size_t thread_count)
    : queue_(queue), metrics_(metrics) {
  if (thread_count == 0) thread_count = std::thread::hardware_concurrency();
  if (thread_count == 0) thread_count = 4;

  workers_.reserve(thread_count);
  for (std::size_t i = 0; i < thread_count; ++i) {
    workers_.emplace_back([this](std::stop_token st) { WorkerLoop(st); });
  }
}

ThreadPool::~ThreadPool() {
  queue_.Shutdown();  // wake all blocked Wait() calls immediately
  workers_.clear();   // jthread dtors: request_stop() + join()
}

void ThreadPool::WorkerLoop(std::stop_token stoken) {
  while (true) {
    auto task = queue_.Wait(stoken);

    if (!task) {
      // Stop fired and queue is empty. Drain any remaining tasks so their
      // promises are resolved and TaskHandle::Wait() never blocks forever.
      while (auto t = queue_.TryPop()) {
        metrics_.queue_size.fetch_sub(1, std::memory_order_relaxed);
        t->completed_at = runtime::time::Timestamp::Now();
        t->state.store(TaskState::kCancelled, std::memory_order_release);
        metrics_.cancelled.fetch_add(1, std::memory_order_relaxed);
        try { t->promise.set_value(); } catch (...) {}
      }
      return;
    }

    // Task popped from WorkQueue — it is no longer "queued".
    metrics_.queue_size.fetch_sub(1, std::memory_order_relaxed);
    task->started_at = runtime::time::Timestamp::Now();
    task->state.store(TaskState::kRunning, std::memory_order_release);
    metrics_.running_count.fetch_add(1, std::memory_order_relaxed);

    // Fast path: cancelled while waiting in the queue.
    if (task->cancel_source.IsCancelled()) {
      metrics_.running_count.fetch_sub(1, std::memory_order_relaxed);
      CompleteTask(*task, TaskState::kCancelled);
      continue;
    }

    try {
      CancellationToken token = task->cancel_source.Token();
      task->func(token);

      metrics_.running_count.fetch_sub(1, std::memory_order_relaxed);
      // If func returned early after checking token, mark as cancelled.
      const TaskState final_state = token.IsCancelled()
                                        ? TaskState::kCancelled
                                        : TaskState::kCompleted;
      CompleteTask(*task, final_state);
    } catch (...) {
      metrics_.running_count.fetch_sub(1, std::memory_order_relaxed);
      FailTask(*task);  // must be called inside catch block
    }
  }
}

// Caller must decrement running_count before calling.
void ThreadPool::CompleteTask(Task& task, TaskState final_state) {
  task.completed_at = runtime::time::Timestamp::Now();
  task.state.store(final_state, std::memory_order_release);

  switch (final_state) {
    case TaskState::kCompleted:
      metrics_.completed.fetch_add(1, std::memory_order_relaxed);
      break;
    case TaskState::kCancelled:
    case TaskState::kTimeout:
      metrics_.cancelled.fetch_add(1, std::memory_order_relaxed);
      break;
    default:
      break;
  }

  try { task.promise.set_value(); } catch (...) {}
}

// Caller must decrement running_count before calling.
// std::current_exception() is valid because caller is in a catch block.
void ThreadPool::FailTask(Task& task) {
  task.completed_at = runtime::time::Timestamp::Now();
  task.state.store(TaskState::kFailed, std::memory_order_release);
  metrics_.failed.fetch_add(1, std::memory_order_relaxed);
  try { task.promise.set_exception(std::current_exception()); } catch (...) {}
}

}  // namespace runtime::task
