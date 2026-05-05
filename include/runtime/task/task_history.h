#pragma once

#include "runtime/task/task_record.h"

#include <cstddef>
#include <deque>
#include <mutex>
#include <vector>

namespace runtime::task {

// Thread-safe fixed-capacity ring buffer of TaskRecords.
// When full, the oldest entry is evicted.
// Called from worker threads (Record); read from any thread (Snapshot).
class TaskHistory {
 public:
  explicit TaskHistory(std::size_t capacity = 256);

  // Appends a record; evicts oldest if at capacity.
  // Thread-safe: called concurrently from worker threads.
  void Record(const Task& task);

  // Returns a copy of all records, oldest first.
  // Thread-safe: safe to call from any thread.
  std::vector<TaskRecord> Snapshot() const;

  std::size_t Capacity() const { return capacity_; }

 private:
  std::size_t capacity_;
  mutable std::mutex mutex_;
  std::deque<TaskRecord> records_;
};

}  // namespace runtime::task
