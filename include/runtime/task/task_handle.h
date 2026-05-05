#pragma once

#include "runtime/task/task_state.h"

#include <cstdint>
#include <future>
#include <memory>

namespace runtime::task {

struct Task;

// TaskHandle is the caller's interface to a submitted task
// Returned by Scheduler::Submit(). Move-only; cannnot be copied.
class TaskHandle {
public:
  TaskHandle() = default;
  TaskHandle(uint64_t id, std::shared_ptr<Task> task, std::future<void> future);

  TaskHandle(const TaskHandle&) = delete;
  TaskHandle&operator=(const TaskHandle&) = delete;
  TaskHandle(TaskHandle&&) = default;
  TaskHandle&operator=(TaskHandle&&) = default;

  uint64_t Id() const { return id_; }
  TaskState State() const;
  bool Valid() const { return task_ != nullptr; }

  void Wait();
  bool Cancel();

private:
  uint64_t id_{0};
  std::shared_ptr<Task> task_;
  std::future<void> future_;
};

}  // namespace runtime::task
