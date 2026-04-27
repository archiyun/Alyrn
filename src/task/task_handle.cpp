#include "runtime/task/task_handle.h"
#include "runtime/task/task.h"

namespace runtime::task {

TaskHandle::TaskHandle(uint64_t id, std::shared_ptr<Task> task, std::future<void> future)
  : id_(id), task_(std::move(task)), future_(std::move(future)) {}

TaskState TaskHandle::State() const {
  if (!task_) return TaskState::kCompleted;
  return task_->state.load(std::memory_order_acquire);
}

void TaskHandle::Wait() {
  if (future_.valid()) {
    future_.get();
  }
}

bool TaskHandle::Cancel() {
  if (!task_) return false;
  const auto current = task_->state.load(std::memory_order_acquire);
  if (current == TaskState::kCompleted ||
  current == TaskState::kFailed ||
  current == TaskState::kCancelled || 
  current == TaskState::kTimeout) {
    return false;
  } 
  task_->cancel_source.Cancel();
  return true;
}
} // namespace runtime::task