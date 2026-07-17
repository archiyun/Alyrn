#pragma once

#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <stop_token>

#include "vexo/base/current_thread.h"
#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/work.h"
#include "vexo/luring/op.h"
#include "vexo/luring/options.h"
#include "vexo/luring/ring.h"

namespace vexo::luring {

class LUringLoop final : public coro::Scheduler {
public:
  LUringLoop();

  [[nodiscard]] base::Result<void> Init(const LUringOptions& options) noexcept;

  void Loop(std::stop_token token) noexcept;
  void Quit() noexcept;
  [[nodiscard]] bool IsInLoopThread() const noexcept { return thread_id_ == base::tid(); }
  [[nodiscard]] int thread_id() const noexcept { return thread_id_; }

  [[nodiscard]] std::size_t PendingSubmitCount() const noexcept { return pending_submit_; }

  [[nodiscard]] std::size_t InflightCount() const noexcept { return inflight_; }

  [[nodiscard]] bool IsDrained() const noexcept { return pending_submit_ == 0 && inflight_ == 0; }

  void Schedule(coro::Work* work) noexcept override;

  template <class Prep>
  [[nodiscard]] base::Result<void> SubmitOp(LUringOp* op, Prep&& prep) noexcept {
    assert(IsInLoopThread());

    io_uring_sqe* sqe = ring_.GetSqe();
    if (sqe == nullptr) {
      auto flushed = FlushSubmit();
      if (!flushed.has_value()) {
        return std::unexpected(flushed.error());
      }

      sqe = ring_.GetSqe();
      if (sqe == nullptr) {
        return std::unexpected(base::make_errno(ENOSPC));
      }
    }

    prep(sqe);
    io_uring_sqe_set_data(sqe, op);
    ++pending_submit_;
    return {};
  }

  [[nodiscard]] base::Result<void> FlushSubmit() noexcept;
  [[nodiscard]] base::Result<std::size_t> PollCompletions() noexcept;
  [[nodiscard]] base::Result<std::size_t> WaitCompletions() noexcept;

  void RunReady() noexcept;
  void RunUntilIdle();

private:
  void HandleCqe(io_uring_cqe* cqe) noexcept;

  const int thread_id_;
  LUringRing ring_;
  coro::WorkQueue ready_;
  std::size_t pending_submit_{0};
  std::size_t inflight_{0};
  std::size_t submit_batch_{32};
  std::atomic_bool quit_{false};
};

}  // namespace vexo::luring
