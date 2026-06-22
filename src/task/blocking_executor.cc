// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/task/blocking_executor.h"

#include <algorithm>
#include <memory>
#include <stdexcept>

#include "vexo/task/detail/task.h"
#include "vexo/task/detail/thread_pool.h"
#include "vexo/task/detail/work_queue.h"

namespace vexo::task {

std::atomic<uint64_t> BlockingExecutor::next_id_{1};

struct BlockingExecutor::Impl {
  Impl(std::size_t worker_count, std::size_t max_queue_size)
      : queue(max_queue_size), pool(queue, metrics, worker_count) {}

  // Declaration order = construction/destruction order. pool holds references
  // to queue and metrics, so it must be declared after them.
  ExecutorMetrics metrics;
  WorkQueue queue;
  ThreadPool pool;
};

BlockingExecutor::BlockingExecutor(std::size_t worker_count,
                                   std::size_t max_queue_size)
    : impl_(std::make_unique<Impl>(worker_count, max_queue_size)) {}

BlockingExecutor::~BlockingExecutor() = default;

TaskHandle BlockingExecutor::Submit(TaskFunction func) {
  SubmitError error = SubmitError::kQueueFull;
  auto handle = TrySubmit(std::move(func), &error);
  if (handle) {
    return std::move(*handle);
  }
  switch (error) {
    case SubmitError::kQueueFull:
      throw std::runtime_error("BlockingExecutor: queue full, task rejected");
    case SubmitError::kShuttingDown:
      throw std::runtime_error("BlockingExecutor: queue shut down");
  }
  throw std::runtime_error("BlockingExecutor: task rejected");
}

std::optional<TaskHandle> BlockingExecutor::TrySubmit(TaskFunction func,
                                                      SubmitError* error) {
  const uint64_t id = next_id_.fetch_add(1, std::memory_order_relaxed);
  auto task = std::make_shared<Task>(id, std::move(func));

  auto future = task->promise.get_future();
  auto task_ref = task;  // keep a ref before moving into queue

  impl_->metrics.submitted.fetch_add(1, std::memory_order_relaxed);
  impl_->metrics.queue_size.fetch_add(1, std::memory_order_relaxed);

  const auto push_status = impl_->queue.TryPush(std::move(task));
  if (push_status != WorkQueue::PushStatus::kOk) {
    impl_->metrics.submitted.fetch_sub(1, std::memory_order_relaxed);
    impl_->metrics.queue_size.fetch_sub(1, std::memory_order_relaxed);
    if (error) {
      *error = push_status == WorkQueue::PushStatus::kFull
                   ? SubmitError::kQueueFull
                   : SubmitError::kShuttingDown;
    }
    return std::nullopt;
  }

  return TaskHandle(id, std::move(task_ref), std::move(future));
}

std::size_t BlockingExecutor::pending_count() const {
  return static_cast<std::size_t>(
      std::max(0, impl_->metrics.queue_size.load(std::memory_order_relaxed)));
}

const ExecutorMetrics& BlockingExecutor::metrics() const {
  return impl_->metrics;
}

}  // namespace vexo::task
