#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/task/scheduler_metrics.h"
#include "runtime/task/task.h"
#include "runtime/task/task_handle.h"
#include "runtime/task/task_history.h"
#include "runtime/task/task_options.h"
#include "runtime/task/thread_pool.h"
#include "runtime/task/work_queue.h"
#include "runtime/task/timer_scheduler.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace runtime::task {

// Scheduler is the single public entry point for task submission.
// It assigns IDs, builds Tasks, enforces queue limits, and returns TaskHandles.
class Scheduler : public runtime::base::NonCopyable {
 public:
  // worker_count == 0 → hardware_concurrency.
  // max_queue_size == 0 → unbounded queue.
  explicit Scheduler(std::size_t worker_count   = 0,
                     std::size_t max_queue_size = 0);

  // Primary submit: func receives a CancellationToken for cooperative cancel.
  TaskHandle Submit(Task::Func func, TaskOptions opts = {});

  // Convenience overload: wraps a void() lambda so callers don't need to
  // accept a CancellationToken when they don't need cooperative cancellation.
  template <typename F>
  TaskHandle Submit(F&& f, TaskOptions opts = {})
      requires(!std::same_as<std::decay_t<F>, Task::Func> &&
                std::invocable<F>) {
    return Submit(
        [f = std::forward<F>(f)](CancellationToken) mutable { f(); },
        std::move(opts));
  }

  std::size_t PendingCount() const {
    return static_cast<std::size_t>(
        std::max(0, metrics_.queue_size.load(std::memory_order_relaxed)));
  }

  const SchedulerMetrics& Metrics() const { return metrics_; }
  const TaskHistory&      History()  const { return history_; }

 private:
  std::size_t max_queue_size_{0};

  // Declaration order = construction order.
  // pool_ holds references to queue_, metrics_, and history_, so they must come first.
  SchedulerMetrics metrics_;
  TaskHistory      history_;
  WorkQueue        queue_;
  TimerScheduler   timer_scheduler_;
  ThreadPool       pool_;

  static std::atomic<uint64_t> next_id_;
};

}  // namespace runtime::task
