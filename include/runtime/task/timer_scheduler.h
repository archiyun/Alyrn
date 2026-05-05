#pragma once

#include "runtime/time/timestamp.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <stop_token>
#include <thread>
#include <vector>

namespace runtime::task {

struct Task;

class TimerScheduler {

public:
  TimerScheduler();
  // jthread dtor calls request_stop() + join() automatically
  ~TimerScheduler() = default;
  
  // Registers a soft timeout. Safe to call from any thread.
  void Schedule(std::weak_ptr<Task> task, uint32_t timeout_ms);

private:
  struct Entry {
    runtime::time::Timestamp deadline;
    std::weak_ptr<Task> task;

    bool operator>(const Entry &o) const { return deadline > o.deadline; }
  };
  void Run(std::stop_token stoken);
  void FireTimeout(const std::weak_ptr<Task> &weak_task);

  std::mutex mutex_;
  std::condition_variable_any cv_;
  std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> heap_;
  std::jthread thread_;
};
} // namespace runtime::task