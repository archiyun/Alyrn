#include "runtime/task/scheduler.h"
#include "runtime/time/timestamp.h"
#include <stdexcept>

namespace runtime::task {

std::atomic<uint64_t> Scheduler::next_id_{1};

Scheduler::Scheduler(std::size_t worker_count, std::size_t max_queue_size) 
  : max_queue_size_(max_queue_size),
    pool_(queue_, metrics_, worker_count) {}

TaskHandle Scheduler::Submit(Task::Func func, TaskOptions opts) {
  if (max_queue_size_ > 0 && metrics_.queue_size.load(std::memory_order_relaxed) 
  >= static_cast<int32_t>(max_queue_size_)) {
    throw std::runtime_error("Scheduler: queue full, task rejected");
  }

  const uint64_t id = next_id_.fetch_add(1, std::memory_order_relaxed);
  auto task = std::make_shared<Task>(
    id, std::move(opts.name), opts.priority, std::move(func)
  );

  const auto now   = runtime::time::Timestamp::Now();
  task->created_at  = now;
  task->enqueued_at = now;

  auto future = task->promise.get_future();

  metrics_.submitted.fetch_add(1, std::memory_order_relaxed);
  metrics_.queue_size.fetch_add(1, std::memory_order_relaxed);

  if (!queue_.Push(std::move(task))) {
    metrics_.queue_size.fetch_sub(1, std::memory_order_relaxed);
    throw std::runtime_error("Scheduler: queue shut down");
  }

  return TaskHandle(id, std::move(task), std::move(future));
}
} // namespace runtime::task