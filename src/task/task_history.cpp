#include "runtime/task/task_history.h"

#include "runtime/task/task.h"

namespace runtime::task {

TaskRecord TaskRecord::From(const Task& task) {
  return TaskRecord{
      task.id,
      task.name,
      task.priority,
      task.state.load(std::memory_order_acquire),
      task.created_at,
      task.enqueued_at,
      task.started_at,
      task.completed_at,
  };
}

TaskHistory::TaskHistory(std::size_t capacity) : capacity_(capacity) {}

void TaskHistory::Record(const Task& task) {
  auto rec = TaskRecord::From(task);
  std::lock_guard lk{mutex_};
  if (records_.size() >= capacity_) {
    records_.pop_front();
  }
  records_.push_back(std::move(rec));
}

std::vector<TaskRecord> TaskHistory::Snapshot() const {
  std::lock_guard lk{mutex_};
  return {records_.begin(), records_.end()};
}

}  // namespace runtime::task
