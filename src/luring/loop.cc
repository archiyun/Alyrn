// SPDX-License-Identifier: MIT

#include "coropact/luring/loop.h"

#include <liburing.h>
#include <liburing/io_uring.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <expected>
#include <stop_token>
#include <thread>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/current_thread.h"
#include "coropact/base/try.h"
#include "coropact/coro/scheduler.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/detail/provided_buffer_pool.h"
#include "coropact/luring/detail/sqe_prep.h"
#include "coropact/luring/detail/ring.h"
#include "coropact/luring/options.h"
#include "coropact/result.h"
#include "coropact/time/clock.h"

namespace coropact::luring {

using namespace detail;

namespace {

constexpr std::size_t kMaxCqesPerTurn = 256;
constexpr std::size_t kMaxReadyWorkPerTurn = 256;
constexpr std::size_t kMaxCompletionWorkPerTurn = 64;

constexpr std::chrono::milliseconds kStopPollInterval{100};

[[nodiscard]]
LUringOp* DecodeOp(io_uring_cqe* cqe) noexcept {
  return reinterpret_cast<LUringOp*>(io_uring_cqe_get_data(cqe));
}

}  // namespace

namespace detail {

CompletionDisposition DispatchCompletion(LUringOp* op, CompletionEvent event) noexcept {
  COROPACT_CHECK(op != nullptr, "cannot dispatch a null LUringOp");

  switch (op->DispatchKind()) {
    case LUringOpKind::kAcceptComplete:
      DispatchAcceptComplete(op);
      break;
    case LUringOpKind::kConnect:
      DispatchConnectComplete(op);
      break;
    case LUringOpKind::kListenerCloseComplete:
      DispatchListenerCloseComplete(op);
      break;
    case LUringOpKind::kReadComplete:
      DispatchStreamReadComplete(op);
      break;
    case LUringOpKind::kReadIntoComplete:
      DispatchStreamReadIntoComplete(op);
      break;
    case LUringOpKind::kTimedReadComplete:
      DispatchTimedReadComplete(op);
      break;
    case LUringOpKind::kTimedReadTimeoutComplete:
      DispatchTimedReadTimeoutComplete(op);
      break;
    case LUringOpKind::kWriteComplete:
      DispatchStreamWriteComplete(op);
      break;
    case LUringOpKind::kStreamCloseComplete:
      DispatchStreamCloseComplete(op);
      break;
    case LUringOpKind::kTimerDriverComplete:
      DispatchTimerDriverComplete(op);
      break;
    case LUringOpKind::kTimerControlComplete:
      DispatchTimerControlComplete(op);
      break;
    case LUringOpKind::kAcceptSourceComplete:
      return DispatchAcceptSourceComplete(op, event);
    case LUringOpKind::kAcceptSourceCancelComplete:
      DispatchAcceptSourceCancelComplete(op);
      break;
    case LUringOpKind::kRecvSourceComplete:
      return DispatchRecvSourceComplete(op, event);
    case LUringOpKind::kSendZeroCopyComplete:
      return DispatchSendZeroCopyComplete(op, event);
    case LUringOpKind::kRecvSourceCancelComplete:
      DispatchRecvSourceCancelComplete(op);
      break;
    case LUringOpKind::kNone:
    case LUringOpKind::kMsgRing:
    case LUringOpKind::kWake:
    case LUringOpKind::kCancelAll:
    case LUringOpKind::kNop:
      break;
    case LUringOpKind::kCount:
      break;
  }

  return CompletionDisposition{
      .kernel_request_terminal = true,
      .decrement_inflight = true,
      .resume_continuation = op->resume_work.HasHandle(),
  };
}

}  // namespace detail

LUringLoop::LUringLoop(std::pmr::memory_resource* frame_resource)
    : Scheduler(frame_resource), thread_id_(base::CurrentThreadId()), timers_(this) {}

LUringLoop::~LUringLoop() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop destroyed from wrong thread");
  if (initialized_) {
    COROPACT_CHECK(IsDrained(), "LUringLoop destroyed with pending user operation work");
  }
  if (wake_fd_ >= 0) {
    ::close(wake_fd_);
  }
}

Result<void> LUringLoop::Init(const LUringOptions& options) noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::Init called from wrong thread");

  if (initialized_) {
    return std::unexpected(Errno(EALREADY));
  }

  COROPACT_TRY_VALUE(ring, LUringRing::Create(options));
  ring_ = std::move(ring);
  wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wake_fd_ < 0) {
    return std::unexpected(CurrentErrno());
  }
  pending_submit_ = 0;
  inflight_ = 0;
  wake_pending_ = false;
  wake_inflight_ = false;
  cancel_all_pending_ = false;
  shared_buffer_pool_.reset();
  shared_buffer_capacity_ = options.shared_buffer_capacity;
  shared_buffer_size_ = options.shared_buffer_size;
  cancel_all_op_.BeginNextRequest();
  initialized_ = true;
  auto armed = ArmWakePoll();
  if (!armed.has_value()) {
    initialized_ = false;
    ::close(std::exchange(wake_fd_, -1));
    return std::unexpected(armed.error());
  }
  return {};
}

Result<detail::ProvidedBufferPool*> LUringLoop::GetSharedProvidedBufferPool(
    std::size_t buffer_size, std::size_t source_capacity) noexcept {
  COROPACT_CHECK(IsInLoopThread(),
                 "LUringLoop::GetSharedProvidedBufferPool called from wrong thread");
  if (shared_buffer_capacity_ == 0) {
    return std::unexpected(Errno(ENOENT));
  }
  if (source_capacity == 0 || source_capacity > shared_buffer_capacity_) {
    return std::unexpected(Errno(EINVAL));
  }
  if (buffer_size != shared_buffer_size_) {
    return std::unexpected(Errno(EINVAL));
  }
  if (shared_buffer_pool_ != nullptr) {
    shared_buffer_pool_->EnsurePublished(source_capacity);
    return shared_buffer_pool_.get();
  }
  auto group = AllocateBufferGroupId();
  if (!group.has_value()) {
    return std::unexpected(group.error());
  }
  auto pool = detail::ProvidedBufferPool::Create(ring_.Native(), *group, shared_buffer_capacity_,
                                                 shared_buffer_size_, source_capacity);
  if (!pool.has_value()) {
    ReleaseBufferGroupId(*group);
    return std::unexpected(pool.error());
  }
  try {
    shared_buffer_pool_ = std::make_unique<detail::ProvidedBufferPool>(std::move(*pool));
  } catch (...) {
    ReleaseBufferGroupId(*group);
    return std::unexpected(Errno(ENOMEM));
  }
  return shared_buffer_pool_.get();
}

Result<std::unique_ptr<detail::ProvidedBufferPool>> LUringLoop::CreateIncrementalProvidedBufferPool(
    std::size_t buffer_size, std::size_t source_capacity) noexcept {
  COROPACT_CHECK(IsInLoopThread(),
                 "LUringLoop::CreateIncrementalProvidedBufferPool called from wrong thread");
  if (shared_buffer_capacity_ == 0) {
    return std::unexpected(Errno(ENOENT));
  }
  if (source_capacity == 0 || source_capacity > shared_buffer_capacity_ ||
      buffer_size != shared_buffer_size_) {
    return std::unexpected(Errno(EINVAL));
  }

  auto group = AllocateBufferGroupId();
  if (!group.has_value()) {
    return std::unexpected(group.error());
  }
  auto pool = detail::ProvidedBufferPool::Create(ring_.Native(), *group, source_capacity,
                                                 buffer_size, source_capacity, true);
  if (!pool.has_value()) {
    ReleaseBufferGroupId(*group);
    return std::unexpected(pool.error());
  }
  try {
    return std::make_unique<detail::ProvidedBufferPool>(std::move(*pool));
  } catch (...) {
    ReleaseBufferGroupId(*group);
    return std::unexpected(Errno(ENOMEM));
  }
}

void LUringLoop::Run(std::stop_token token) noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::Run called from wrong thread");

  if (!initialized_) {
    return;
  }

  backend::LoopState expected = backend::LoopState::kCreated;
  if (!state_.compare_exchange_strong(expected, backend::LoopState::kRunning,
                                      std::memory_order_acq_rel, std::memory_order_acquire) &&
      expected != backend::LoopState::kStopping) {
    return;
  }

  std::stop_callback on_stop{token, [this] { RequestStop(); }};
  while (State() == backend::LoopState::kRunning) {
    // Observe already available completions before spending the turn on
    // ready work. This prevents a ready backlog from delaying CQE handling.
    auto completed = PollCompletions();
    if (!completed.has_value()) {
      RequestStop();
      break;
    }

    if (State() != backend::LoopState::kRunning) {
      break;
    }

    RunReady();

    if (*completed == 0 && !HasReadyWork() && inflight_ > 0) {
      completed = WaitCompletionsFor(kStopPollInterval);
      if (!completed.has_value() && completed.error().value() != ETIME) {
        RequestStop();
        break;
      }
    }
  }

  if (State() == backend::LoopState::kStopping) {
    DrainStoppedOperations();
    // Physical timeout requests are terminal after the drain. Logical timers
    // that have not expired are now canceled by loop shutdown and may release
    // their callbacks without running them.
    timers_.DiscardAll();
  }
  state_.store(backend::LoopState::kStopped, std::memory_order_release);
}

void LUringLoop::RequestStop() noexcept {
  backend::LoopState observed = state_.load(std::memory_order_acquire);
  while (observed == backend::LoopState::kCreated || observed == backend::LoopState::kRunning) {
    if (state_.compare_exchange_weak(observed, backend::LoopState::kStopping,
                                     std::memory_order_acq_rel, std::memory_order_acquire)) {
      Wake();
      return;
    }
  }
}

Result<void> LUringLoop::CancelPendingOperations() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::CancelPendingOperations called from wrong thread");

  if (cancel_all_pending_ || (PendingSubmitCount() == 0 && InflightCount() == 0)) {
    return {};
  }

  cancel_all_op_.BeginNextRequest();

  auto submitted = SubmitOp(&cancel_all_op_, detail::PrepareCancelAll());
  if (submitted.has_value()) {
    cancel_all_pending_ = true;
  }
  return submitted;
}

void LUringLoop::DrainStoppedOperations() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::DrainStoppedOperations called from wrong thread");

  while (!IsDrained()) {
    RunReady();
    if (IsDrained()) {
      break;
    }

    auto cancelled = CancelPendingOperations();
    if (!cancelled.has_value()) {
      // Run() has no error return channel, and publishing Stopped with a
      // live ring request would violate its drain contract. A local cancel
      // preparation failure therefore cannot end shutdown: reap any work
      // that is already in flight and retry the cancellation on a later turn.
      auto completed = PollCompletions();
      if (!completed.has_value() || *completed == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      continue;
    }

    if (PendingSubmitCount() == 0 && InflightCount() == 0) {
      continue;
    }

    auto completed = PollCompletions();
    if (!completed.has_value()) {
      // FlushSubmit() or CQ reaping can fail after a cancel SQE has already
      // been prepared. Keep the pending SQEs and retry rather than publishing
      // Stopped with requests that the ring may still observe.
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }
    if (*completed == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  RunReady();
}

void LUringLoop::Schedule(coro::Work* work) noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::Schedule called from wrong thread");
  ready_.PushBack(work);
}

void LUringLoop::ScheduleCompletion(coro::Work* work) noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::ScheduleCompletion called from wrong thread");
  completion_ready_.PushBack(work);
}

void LUringLoop::RunReady() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::RunReady called from wrong thread");
  ExecutionScope execution_scope{*this};

  std::size_t resumed = 0;
  std::size_t completion_resumed = 0;
  while (HasReadyWork() && resumed < kMaxReadyWorkPerTurn) {
    coro::Work* work = nullptr;
    const bool run_completion = !completion_ready_.Empty() &&
                                (ready_.Empty() || completion_resumed < kMaxCompletionWorkPerTurn);
    if (run_completion) {
      work = completion_ready_.PopFront();
      ++completion_resumed;
    } else {
      work = ready_.PopFront();
    }
    RunInExecutionScope(work);
    ++resumed;
  }
}

Result<void> LUringLoop::FlushSubmit() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::FlushSubmit called from wrong thread");

  while (pending_submit_ > 0) {
    COROPACT_TRY_VALUE(submitted, ring_.Submit());
    if (submitted == 0) {
      return std::unexpected(Errno(EAGAIN));
    }

    const std::size_t count = std::min(submitted, pending_submit_);
    pending_submit_ -= count;
    inflight_ += count;
    if (wake_pending_ && count > 0) {
      wake_pending_ = false;
      wake_inflight_ = true;
    }
  }

  return {};
}

Result<std::size_t> LUringLoop::PollCompletions() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::PollCompletions called from wrong thread");

  COROPACT_TRY(FlushSubmit());

  return ring_.Reap([this](io_uring_cqe* cqe) { HandleCqe(cqe); }, kMaxCqesPerTurn);
}

Result<std::size_t> LUringLoop::WaitCompletions() noexcept {
  return WaitCompletionsFor(std::chrono::nanoseconds::max());
}

Result<std::size_t> LUringLoop::WaitCompletionsFor(std::chrono::nanoseconds timeout) noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::WaitCompletionsFor called from wrong thread");

  COROPACT_TRY(FlushSubmit());

  io_uring_cqe* cqe = nullptr;
  int r = 0;
  if (timeout == std::chrono::nanoseconds::max()) {
    r = io_uring_wait_cqe(ring_.Native(), &cqe);
  } else {
    constexpr std::int64_t kNanosecondsPerSecond = 1'000'000'000;
    const std::int64_t count = timeout.count();
    __kernel_timespec timeout_spec{};
    timeout_spec.tv_sec = count / kNanosecondsPerSecond;
    timeout_spec.tv_nsec = count % kNanosecondsPerSecond;
    r = io_uring_wait_cqe_timeout(ring_.Native(), &cqe, &timeout_spec);
  }
  if (r < 0) {
    return std::unexpected(NegErrno(r));
  }

  return ring_.Reap([this](io_uring_cqe* completed_cqe) { HandleCqe(completed_cqe); },
                    kMaxCqesPerTurn);
}

void LUringLoop::HandleCqe(io_uring_cqe* cqe) noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::HandleCqe called from wrong thread");

  if (cqe->user_data == kMsgRingNotificationUserData) {
    HandleMailbox();
    return;
  }

  LUringOp* op = DecodeOp(cqe);
  if (op == nullptr) {
    if (inflight_ > 0) {
      --inflight_;
    }
    return;
  }

  const CompletionEvent event{cqe->res, cqe->flags};

  const auto apply_disposition = [this, op](CompletionDisposition disposition) noexcept {
    if (disposition.kernel_request_terminal) {
      COROPACT_CHECK(disposition.decrement_inflight,
                     "terminal LUring completion must decrement inflight work");
    }
    if (disposition.decrement_inflight) {
      COROPACT_CHECK(inflight_ > 0, "LUringLoop inflight count underflow");
      --inflight_;
    }
    if (disposition.resume_continuation) {
      if (UsesCoupledSingleResultLifecycle(op->DispatchKind())) {
        COROPACT_CHECK(op->TryAuthorizeCoupledContinuation(),
                       "coupled LUring operation resumed before release authorization");
      }
      const bool resume_gate_won =
          op->CqeCompletionRecorded() || op->TryMarkCompletionWithoutCqeResult();
      if (resume_gate_won && op->resume_work.HasHandle()) {
        ScheduleCompletion(&op->resume_work);
      }
    }
  };

  if (op == &wake_op_) {
    apply_disposition(CompletionDisposition{
        .kernel_request_terminal = true,
        .decrement_inflight = true,
        .resume_continuation = false,
    });
    wake_inflight_ = false;
    DrainWakeFd();
    if (State() == backend::LoopState::kRunning) {
      wake_op_.BeginNextRequest();
      auto armed = ArmWakePoll();
      if (!armed.has_value()) {
        RequestStop();
      }
    }
    return;
  }

  if (op == &cancel_all_op_) {
    cancel_all_pending_ = false;
    (void)(op->TryRecordCqeCompletion(cqe->res));
    apply_disposition(detail::DispatchCompletion(op, event));
    return;
  }

  if (CompletionModelFor(op->DispatchKind()) == LUringCompletionModel::kSingleShot &&
      !op->TryRecordCqeCompletion(cqe->res)) {
    return;
  }

  // For coupled single-shot awaiters, TryRecordCqeCompletion() settles the
  // physical CQE before DispatchCompletion() runs. Read/write CQEs directly
  // authorize result readiness; stream-valued Accept and Connect first refine
  // the CQE into Result<Stream>. Their adapter then crosses its backend-owned
  // release boundary; only apply_disposition() can authorize continuation
  // resumption. This is the luring refinement of:
  //
  //   result ready -> release -> continuation resume
  //
  // Composite and split-release operations return an explicit disposition and
  // retain their own stricter lifecycle state machines.
  apply_disposition(detail::DispatchCompletion(op, event));
}

Result<void> LUringLoop::ArmWakePoll() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::ArmWakePoll called from wrong thread");
  if (wake_fd_ < 0) {
    return std::unexpected(Errno(EBADF));
  }

  wake_op_.kind = LUringOpKind::kWake;
  wake_op_.resume_work.ClearHandle();
  auto submitted = SubmitOp(&wake_op_, detail::PreparePollAdd(wake_fd_, POLLIN));
  if (submitted.has_value()) {
    wake_pending_ = true;
  }
  return submitted;
}

void LUringLoop::DrainWakeFd() noexcept {
  if (wake_fd_ < 0) {
    return;
  }

  std::uint64_t value = 0;
  while (::read(wake_fd_, &value, sizeof(value)) == sizeof(value)) {
  }
}

void LUringLoop::Wake() noexcept {
  if (wake_fd_ < 0) {
    return;
  }

  constexpr std::uint64_t kWakeValue = 1;
  const ssize_t written = ::write(wake_fd_, &kWakeValue, sizeof(kWakeValue));
  if (written < 0 && errno != EAGAIN && errno != EINTR) {
    RequestStop();
  }
}

void LUringLoop::HandleMailbox() noexcept {
  COROPACT_CHECK(IsInLoopThread(), "LUringLoop::HandleMailbox called from wrong thread");

  mailbox_.Drain([this](const LUringMessage& message) noexcept {
    auto* work = reinterpret_cast<coro::Work*>(static_cast<std::uintptr_t>(message.data));
    COROPACT_CHECK(work != nullptr, "mailbox message contains a null work pointer");
    ScheduleCompletion(work);
  });
}

}  // namespace coropact::luring
