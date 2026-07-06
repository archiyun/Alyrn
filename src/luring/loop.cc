#include "vexo/luring/loop.h"

#include <liburing.h>

#include <cerrno>
#include <cstdint>
#include <expected>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/luring/op.h"
#include "vexo/luring/options.h"
#include "vexo/luring/ring.h"

namespace vexo::luring {

namespace {

[[nodiscard]] LUringOp* DecodeOp(io_uring_cqe* cqe) noexcept {
  return reinterpret_cast<LUringOp*>(io_uring_cqe_get_data(cqe));
}

}  // namespace

base::Result<void> LUringLoop::Init(const LUringOptions& options) noexcept {
  auto ring = LUringRing::Create(options);
  if (!ring.has_value()) {
    return std::unexpected(ring.error());
  }

  ring_ = std::move(*ring);
  submit_batch_ = options.submit_batch == 0 ? 1 : options.submit_batch;
  pending_submit_ = 0;
  return {};
}

void LUringLoop::Schedule(coro::Work* work) noexcept { ready_.PushBack(work); }

void LUringLoop::RunReady() noexcept {
  coro::Scheduler* previous = coro::Scheduler::Current();
  coro::Scheduler::SetCurrent(this);

  while (!ready_.empty()) {
    coro::Work* work = ready_.PopFront();
    work->Run();
  }

  coro::Scheduler::SetCurrent(previous);
}

base::Result<void> LUringLoop::FlushSubmit() noexcept {
  if (pending_submit_ == 0) return {};
  pending_submit_ = 0;
  return ring_.Submit();
}

base::Result<std::size_t> LUringLoop::PollCompletions() noexcept {
  auto flushed = FlushSubmit();
  if (!flushed.has_value()) {
    return std::unexpected(flushed.error());
  }
  return ring_.Reap([this](io_uring_cqe* cqe) { HandleCqe(cqe); });
}

base::Result<std::size_t> LUringLoop::WaitCompletions() noexcept {
  auto flushed = FlushSubmit();
  if (!flushed.has_value()) {
    return std::unexpected(flushed.error());
  }

  io_uring_cqe* cqe = nullptr;
  int r = io_uring_wait_cqe(ring_.native(), &cqe);
  if (r < 0) {
    return std::unexpected(base::make_neg_errno(r));
  }

  std::size_t n = 0;
  unsigned head = 0;
  io_uring_for_each_cqe(ring_.native(), head, cqe) {
    HandleCqe(cqe);
    ++n;
  }
  io_uring_cq_advance(ring_.native(), static_cast<unsigned>(n));
  return n;
}

void LUringLoop::HandleCqe(io_uring_cqe* cqe) noexcept {
  LUringOp* op = DecodeOp(cqe);
  if (op == nullptr) return;

  op->Complete(cqe->res);
  if (op->resume_work.handle) {
    Schedule(&op->resume_work);
  }
}

}  // namespace vexo::luring
