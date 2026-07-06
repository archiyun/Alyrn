#pragma once

#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/work.h"
#include "vexo/luring/op.h"
#include "vexo/luring/options.h"
#include "vexo/luring/ring.h"

namespace vexo::luring {

class LUringLoop final : public coro::Scheduler {
public:
  LUringLoop() = default;

  [[nodiscard]] base::Result<void> Init(const LUringOptions& options) noexcept;

  void Schedule(coro::Work* work) noexcept override;

  template <class Prep>
  [[nodiscard]] base::Result<void> SubmitOp(LUringOp* op, Prep&& prep) noexcept {
    io_uring_sqe* sqe = ring_.GetSqe();
    if (sqe == nullptr) {
      return std::unexpected(base::make_errno(ENOSPC));
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

  LUringRing ring_;
  coro::WorkQueue ready_;
  std::size_t pending_submit_{0};
  std::size_t submit_batch_{32};
};

}  // namespace vexo::luring
