#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/task/task.h"
#include "runtime/task/thread_pool.h"

#include <atomic>
#include <cstddef>

namespace runtime::task {

// Scheduler adds queue limits and task bookkeeping on top of ThreadPool.
class Scheduler : public runtime::base::NonCopyable {
public:
  // worker_count == 0 uses hardware concurrency.
  // max_queue_size == 0 means the queue is unbounded.
  explicit Scheduler(std::size_t worker_count = 0,
                     std::size_t max_queue_size = 0);

  void Submit(Task task);

  template <typename Func>
  void Submit(Func&& f) {
    Submit(Task(std::forward<Func>(f)));
  }

  std::size_t PendingCount() const { return pending_.load(); }

private:
  ThreadPool pool_;
  std::size_t max_queue_size_{0};
  std::atomic<std::size_t> pending_{0};
};

}  // namespace runtime::task
