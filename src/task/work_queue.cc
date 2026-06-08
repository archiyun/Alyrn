// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/task/detail/work_queue.h"

namespace runtime::task {

WorkQueue::WorkQueue(std::size_t max_size) : max_size_(max_size) {}

WorkQueue::PushStatus WorkQueue::TryPush(std::shared_ptr<Task>&& task) {
  {
    std::lock_guard lk(mutex_);
    if (shutdown_) return PushStatus::kShutdown;
    if (max_size_ > 0 && queue_.size() >= max_size_) {
      return PushStatus::kFull;
    }
    queue_.push(std::move(task));
  }
  cv_.notify_one();
  return PushStatus::kOk;
}

bool WorkQueue::Push(std::shared_ptr<Task>&& task) {
  return TryPush(std::move(task)) == PushStatus::kOk;
}

std::shared_ptr<Task> WorkQueue::Wait(std::stop_token stoken) {
  std::unique_lock lk(mutex_);
  cv_.wait(lk, stoken, [this] { return shutdown_ || !queue_.empty(); });
  if (queue_.empty()) return nullptr;
  auto task = std::move(queue_.front());
  queue_.pop();
  return task;
}

std::shared_ptr<Task> WorkQueue::TryPop() {
  std::lock_guard lk(mutex_);
  if (queue_.empty()) return nullptr;
  auto task = std::move(queue_.front());
  queue_.pop();
  return task;
}

std::size_t WorkQueue::size() const {
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
