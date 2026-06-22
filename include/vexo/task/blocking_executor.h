// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "vexo/base/noncopyable.h"
#include "vexo/task/cancellation_token.h"
#include "vexo/task/executor_metrics.h"
#include "vexo/task/task_handle.h"

namespace vexo::task {

using TaskFunction = std::function<void(CancellationToken)>;

enum class SubmitError {
  kQueueFull,
  kShuttingDown,
};

// BlockingExecutor runs blocking or CPU-bound callbacks outside I/O loop
// threads. It assigns IDs, enforces queue limits, and returns TaskHandles.
class BlockingExecutor : public vexo::base::NonCopyable {
 public:
  // worker_count == 0 → hardware_concurrency.
  // max_queue_size == 0 → unbounded queue.
  explicit BlockingExecutor(std::size_t worker_count   = 0,
                            std::size_t max_queue_size = 0);
  ~BlockingExecutor();

  // Primary submit: func receives a CancellationToken for cooperative cancel.
  TaskHandle Submit(TaskFunction func);

  // Non-throwing submit. Returns std::nullopt when the executor cannot accept
  // the task; error is filled when provided.
  std::optional<TaskHandle> TrySubmit(TaskFunction func,
                                      SubmitError* error = nullptr);

  // Convenience overload: wraps a void() lambda so callers don't need to
  // accept a CancellationToken when they don't need cooperative cancellation.
  template <typename F>
  TaskHandle Submit(F&& f)
      requires(!std::same_as<std::decay_t<F>, TaskFunction> &&
                std::invocable<F>) {
    return Submit(
        [f = std::forward<F>(f)](CancellationToken) mutable { f(); });
  }

  template <typename F>
  std::optional<TaskHandle> TrySubmit(F&& f, SubmitError* error = nullptr)
      requires(!std::same_as<std::decay_t<F>, TaskFunction> &&
               std::invocable<F>) {
    return TrySubmit(
        [f = std::forward<F>(f)](CancellationToken) mutable { f(); },
        error);
  }

  std::size_t pending_count() const;

  const ExecutorMetrics& metrics() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  static std::atomic<uint64_t> next_id_;
};

}  // namespace vexo::task
