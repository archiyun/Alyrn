#pragma once

#include "runtime/time/timestamp.h"

#include <atomic>
#include <functional>

namespace runtime::task {

// Task wraps one scheduled function together with scheduling metadata.
class Task {
public:
  using Func = std::function<void()>;

  explicit Task(Func f);

  // std::atomic is not movable, so Task defines a custom move constructor.
  Task(Task&& other) noexcept
      : func_(std::move(other.func_)),
        id_(other.id_),
        priority_(other.priority_),
        deadline_(other.deadline_),
        cancelled_(other.cancelled_.load()) {}

  int Id() const { return id_; }
  int Priority() const { return priority_; }
  void SetPriority(int p) { priority_ = p; }

  runtime::time::Timestamp Deadline() const { return deadline_; }
  void SetDeadline(runtime::time::Timestamp t) { deadline_ = t; }

  bool Cancelled() const { return cancelled_.load(); }
  void Cancel() { cancelled_.store(true); }

  void Run();

private:
  Func func_;
  int id_;
  int priority_{0};
  runtime::time::Timestamp deadline_{};
  std::atomic<bool> cancelled_{false};

  static std::atomic<int> id_counter_;
};
}  // namespace runtime::task
