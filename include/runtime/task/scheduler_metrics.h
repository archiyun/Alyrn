#pragma once

#include <atomic>
#include <cstdint>

namespace runtime::task {

struct SchedulerMetrics {
  // Monotonically increasing counters
  std::atomic<uint64_t> submitted{0};
  std::atomic<uint64_t> completed{0};
  std::atomic<uint64_t> failed{0};
  std::atomic<uint64_t> cancelled{0};  // explicit cancel only
  std::atomic<uint64_t> timeout{0};    // soft timeout (separate from cancelled)

  // Current instantaneous values
  std::atomic<int32_t> queue_size{0};     // tasks waiting in WorkQueue
  std::atomic<int32_t> running_count{0};  // tasks currently executing

  // Non-atomic snapshot (values read individually, not as a unit)
  struct Snapshot {
    uint64_t submitted;
    uint64_t completed;
    uint64_t failed;
    uint64_t cancelled;
    uint64_t timeout;
    int32_t  queue_size;
    int32_t  running_count;
  };

  Snapshot Load() const noexcept {
    return {
      submitted.load(std::memory_order_relaxed),
      completed.load(std::memory_order_relaxed),
      failed.load(std::memory_order_relaxed),
      cancelled.load(std::memory_order_relaxed),
      timeout.load(std::memory_order_relaxed),
      queue_size.load(std::memory_order_relaxed),
      running_count.load(std::memory_order_relaxed),
    };
  }
};

}  // namespace runtime::task
