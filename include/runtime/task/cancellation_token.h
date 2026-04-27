#pragma once

#include <atomic>
#include <memory>

namespace runtime::task {

// Read-only view of a cancellation falg.
// Pass by value into Task::func; poll IsCancelled() at yield points
class CancellationToken {
public:
  explicit CancellationToken(std::shared_ptr<std::atomic<bool>> flag)
      : flag_(std::move(flag)) {}

  bool IsCancelled() const { return flag_->load(std::memory_order_relaxed); }

private:
  std::shared_ptr<std::atomic<bool>> flag_;
};

class CancellationSource {
public:
  CancellationSource() : flag_(std::make_shared<std::atomic<bool>>(false)) {}
  void Cancel() { flag_->store(true, std::memory_order_relaxed); }
  bool IsCancelled() const { return flag_->load(std::memory_order_relaxed); }
  CancellationToken Token() const { return CancellationToken(flag_); }

private:
  std::shared_ptr<std::atomic<bool>> flag_;
};
} // namespace runtime::task