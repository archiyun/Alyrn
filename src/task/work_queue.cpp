#include "runtime/task/work_queue.h"

namespace runtime::task {

bool WorkQueue::Push(std::shared_ptr<Task>&& task) {
  {
    std::lock_guard lk(mutex_);
    if (shutdown_) return false;
    queue_.push(std::move(task));
  }
  cv_.notify_one();
  return true;
}

std::shared_ptr<Task> WorkQueue::Wait(std::stop_token stoken) {
  std::unique_lock lk(mutex_);
  cv_.wait(lk, stoken, [this] { return shutdown_ || !queue_.empty(); });
  if (queue_.empty()) return nullptr;
  auto task = std::move(queue_.top());
  queue_.pop();
  return task;
}

std::shared_ptr<Task> WorkQueue::TryPop() {
  std::lock_guard lk(mutex_);
  if (queue_.empty()) return nullptr;
  auto task = queue_.top();
  queue_.pop();
  return task;
}

std::size_t WorkQueue::Size() const {
  std::lock_guard lk(mutex_);
  return queue_.size();
}

void WorkQueue::Shutdown() {
  {
    std::lock_guard lk(mutex_);
    shutdown_ = true;
  }
  cv_.notify_all();
}

}  // namespace runtime::task
