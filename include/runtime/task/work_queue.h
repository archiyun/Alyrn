#pragma once

#include "runtime/task/task.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <stop_token>
#include <vector>

namespace runtime::task {

// Thread-safe priority queue for Tasks.
// Higher TaskPriority integer value → dequeued first.
class WorkQueue {
 public:
  // Returns false if the queue has been shut down.
  bool Push(std::shared_ptr<Task>&& task);

  // Blocks until a task is available or stoken fires.
  // Returns nullptr when the worker should exit.
  std::shared_ptr<Task> Wait(std::stop_token stoken);

  // Non-blocking pop. Returns nullptr if empty.
  std::shared_ptr<Task> TryPop();

  std::size_t Size() const;

  // Wakes all blocked Wait() calls so workers can drain and exit.
  void Shutdown();

 private:
  struct ByPriority {
    // priority_queue is a max-heap: comp(a,b)=true means a has lower priority.
    bool operator()(const std::shared_ptr<Task>& a,
                    const std::shared_ptr<Task>& b) const {
      return static_cast<int>(a->priority) < static_cast<int>(b->priority);
    }
  };

  mutable std::mutex          mutex_;
  std::condition_variable_any cv_;
  std::priority_queue<std::shared_ptr<Task>,
                      std::vector<std::shared_ptr<Task>>,
                      ByPriority> queue_;
  bool shutdown_{false};
};

}  // namespace runtime::task
