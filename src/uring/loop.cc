// SPDX-License-Identifier: MIT

#include "alyrn/uring/loop.h"

#include <liburing.h>
#include <liburing/io_uring.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <memory_resource>
#include <stop_token>
#include <utility>

#include "alyrn/backend/loop.h"
#include "alyrn/coro/frame_allocator.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/work.h"
#include "alyrn/detail/check.h"
#include "alyrn/result.h"
#include "alyrn/uring/detail/completion_dispatch.h"
#include "alyrn/uring/detail/op.h"
#include "alyrn/uring/detail/provided_buffer_pool.h"
#include "alyrn/uring/detail/ring.h"
#include "alyrn/uring/detail/sqe_prep.h"
#include "alyrn/uring/detail/timer_queue.h"
#include "alyrn/uring/options.h"

namespace alyrn::uring {

using namespace detail;

namespace {

constexpr std::size_t kMaxCqesPerTurn = 256;
constexpr std::size_t kMaxReadyWorkPerTurn = 256;
constexpr std::size_t kMaxCompletionWorkPerTurn = 64;

constexpr std::chrono::milliseconds kStopPollInterval{100};

thread_local Loop* t_loop_in_this_thread = nullptr;

Op* DecodeOp(io_uring_cqe* cqe) noexcept {
  return reinterpret_cast<Op*>(io_uring_cqe_get_data(cqe));
}

}  // namespace

namespace detail {

CompletionDisposition DispatchCompletion(Op* op, CompletionEvent event) noexcept {
  ALYRN_CHECK(op != nullptr, "cannot dispatch a null Op");

  switch (op->DispatchKind()) {
    case OpKind::kAcceptComplete:
      DispatchAcceptComplete(op);
      break;
    case OpKind::kConnect:
      DispatchConnectComplete(op);
      break;
    case OpKind::kListenerCloseComplete:
      DispatchListenerCloseComplete(op);
      break;
    case OpKind::kReadComplete:
      DispatchStreamReadComplete(op);
      break;
    case OpKind::kRecvComplete:
      DispatchStreamRecvComplete(op);
      break;
    case OpKind::kRecvCopyComplete:
      DispatchStreamRecvCopyComplete(op, event);
      break;
    case OpKind::kWriteComplete:
      DispatchStreamWriteComplete(op);
      break;
    case OpKind::kStreamCloseComplete:
      DispatchStreamCloseComplete(op);
      break;
    case OpKind::kStreamReadCancelComplete:
      return DispatchStreamReadCancelComplete(op);
    case OpKind::kStreamWriteCancelComplete:
      return DispatchStreamWriteCancelComplete(op);
    case OpKind::kTimerDriverComplete:
      DispatchTimerDriverComplete(op);
      break;
    case OpKind::kTimerControlComplete:
      DispatchTimerControlComplete(op);
      break;
    case OpKind::kAcceptSourceComplete:
      return DispatchAcceptSourceComplete(op, event);
    case OpKind::kAcceptSourceCancelComplete:
      DispatchAcceptSourceCancelComplete(op);
      break;
    case OpKind::kRecvSourceComplete:
      return DispatchRecvSourceComplete(op, event);
    case OpKind::kSendZeroCopyComplete:
      return DispatchSendZeroCopyComplete(op, event);
    case OpKind::kRecvSourceCancelComplete:
      DispatchRecvSourceCancelComplete(op);
      break;
    case OpKind::kNone:
    case OpKind::kWake:
    case OpKind::kCancelAll:
    case OpKind::kNop:
      break;
    case OpKind::kCount:
      break;
  }

  return CompletionDisposition{
      .kernel_request_terminal = true,
      .decrement_inflight = true,
      .resume_continuation = op->resume_work.HasHandle(),
  };
}

}  // namespace detail

Loop::Loop(std::pmr::memory_resource* frame_resource) : Scheduler(frame_resource) {
  ALYRN_CHECK(t_loop_in_this_thread == nullptr, "Loop: only one Loop may exist per thread");
  t_loop_in_this_thread = this;
  timers_ = std::make_unique<TimerQueue>(this);
}

bool Loop::IsInLoopThread() const noexcept { return t_loop_in_this_thread == this; }

Result<time::TimerId> Loop::RunAfter(time::Duration delay, std::function<void()> callback) {
  ALYRN_CHECK(IsInLoopThread(), "Loop::RunAfter called from wrong thread");
  if (!initialized_) {
    return std::unexpected(Errno(EBADF));
  }
  return timers_->AddAfter(delay, std::move(callback));
}

Result<void> Loop::CancelTimer(time::TimerId id) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::CancelTimer called from wrong thread");
  if (!initialized_) {
    return std::unexpected(Errno(EBADF));
  }
  return timers_->Cancel(id);
}

Loop::~Loop() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop destroyed from wrong thread");
  if (initialized_) {
    ALYRN_CHECK(IsDrained(), "Loop destroyed with pending user operation work");
  }
  // TimerQueue::DiscardAll() goes through IsInLoopThread().
  timers_.reset();
  if (wake_fd_ >= 0) {
    ::close(wake_fd_);
  }
  t_loop_in_this_thread = nullptr;
}

Result<void> Loop::Init(const Options& options) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::Init called from wrong thread");

  if (initialized_) {
    return std::unexpected(Errno(EALREADY));
  }

  auto ring = Ring::Create(options);
  if (!ring.HasValue()) {
    return std::unexpected(ring.Error());
  }
  ring_ = std::move(*ring);
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
  defer_task_run_ = options.task_run_mode == TaskRunMode::kDeferred;
  cancel_all_op_.BeginNextRequest();
  initialized_ = true;
  auto armed = ArmWakePoll();
  if (!armed.HasValue()) {
    initialized_ = false;
    ::close(std::exchange(wake_fd_, -1));
    return std::unexpected(armed.Error());
  }
  return {};
}

Result<detail::ProvidedBufferPool*> Loop::GetSharedProvidedBufferPool(
    std::size_t buffer_size, std::size_t source_capacity) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::GetSharedProvidedBufferPool called from wrong thread");
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
  if (!group.HasValue()) {
    return std::unexpected(group.Error());
  }
  auto pool = detail::ProvidedBufferPool::Create(ring_.Native(), *group, shared_buffer_capacity_,
                                                 shared_buffer_size_, source_capacity);
  if (!pool.HasValue()) {
    return std::unexpected(pool.Error());
  }
  try {
    shared_buffer_pool_ = std::make_unique<detail::ProvidedBufferPool>(std::move(*pool));
  } catch (...) {
    return std::unexpected(Errno(ENOMEM));
  }
  return shared_buffer_pool_.get();
}

Result<detail::ProvidedBufferPool*> Loop::GetSharedProvidedBufferPool(
    std::size_t source_capacity) noexcept {
  return GetSharedProvidedBufferPool(shared_buffer_size_, source_capacity);
}

void Loop::Run(std::stop_token token) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::Run called from wrong thread");

  if (!initialized_) {
    return;
  }

  auto expected = backend::LoopState::kCreated;
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
    if (!completed.HasValue()) {
      RequestStop();
      break;
    }

    if (State() != backend::LoopState::kRunning) {
      break;
    }

    RunReady();
    coro::CoroFramePoolResource::DrainCurrent();

    if (*completed == 0 && !HasReadyWork() && inflight_ > 0) {
      completed = WaitCompletionsFor(kStopPollInterval);
      if (!completed.HasValue() && completed.Error().value() != ETIME) {
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
    timers_->DiscardAll();
  }
  state_.store(backend::LoopState::kStopped, std::memory_order_release);
}

void Loop::RequestStop() noexcept {
  backend::LoopState observed = state_.load(std::memory_order_acquire);
  while (observed == backend::LoopState::kCreated || observed == backend::LoopState::kRunning) {
    if (state_.compare_exchange_weak(observed, backend::LoopState::kStopping,
                                     std::memory_order_acq_rel, std::memory_order_acquire)) {
      Wake();
      return;
    }
  }
}

Result<void> Loop::CancelPendingOperations() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::CancelPendingOperations called from wrong thread");

  if (cancel_all_pending_ || (PendingSubmitCount() == 0 && InflightCount() == 0)) {
    return {};
  }

  cancel_all_op_.BeginNextRequest();

  auto submitted = SubmitOp(&cancel_all_op_, detail::PrepareCancelAll());
  if (submitted.HasValue()) {
    cancel_all_pending_ = true;
  }
  return submitted;
}

void Loop::DrainStoppedOperations() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::DrainStoppedOperations called from wrong thread");

  while (!IsDrained()) {
    RunReady();
    if (IsDrained()) {
      break;
    }

    auto cancelled = CancelPendingOperations();
    if (!cancelled.HasValue()) {
      // Run() has no error return channel, and publishing Stopped with a
      // live ring request would violate its drain contract. A local cancel
      // preparation failure therefore cannot end shutdown: wait for any work
      // that is already in flight and retry the cancellation on a later turn.
      // The wait is important for DEFER_TASKRUN, where a non-blocking CQ peek
      // does not enter the kernel to run deferred task work.
      if (defer_task_run_) {
        (void)ring_.GetEvents();
      }
      auto completed = WaitCompletionsFor(kStopPollInterval);
      if (!completed.HasValue() && completed.Error().value() != ETIME) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      continue;
    }

    if (PendingSubmitCount() == 0 && InflightCount() == 0) {
      continue;
    }

    // A deferred-task ring needs an io_uring_enter(GETEVENTS) transition to
    // publish task work and cancellation CQEs. WaitCompletionsFor performs
    // that transition while retaining a bounded wait for the shutdown path.
    if (defer_task_run_) {
      (void)ring_.GetEvents();
    }
    auto completed = WaitCompletionsFor(kStopPollInterval);
    if (!completed.HasValue()) {
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
  coro::CoroFramePoolResource::DrainCurrent();
}

void Loop::Schedule(coro::Work* work) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::Schedule called from wrong thread");
  ready_.PushBack(work);
}

void Loop::ScheduleCompletion(coro::Work* work) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::ScheduleCompletion called from wrong thread");
  completion_ready_.PushBack(work);
}

void Loop::DrainCompletionReady() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::DrainCompletionReady called from wrong thread");
  if (completion_ready_.Empty()) {
    return;
  }

  ExecutionScope execution_scope{*this};
  CheckExecutionScope();
  std::size_t resumed = 0;
  while (resumed < kMaxCompletionWorkPerTurn && !completion_ready_.Empty()) {
    RunInExecutionScopeUnchecked(completion_ready_.PopFront());
    ++resumed;
  }
}

void Loop::OnCqeHandled() noexcept {
  auto flushed = FlushSubmit();
  if (!flushed.HasValue()) {
    RequestStop();
    return;
  }
  DrainCompletionReady();
}

void Loop::RunReady() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::RunReady called from wrong thread");
  ExecutionScope execution_scope{*this};
  CheckExecutionScope();

  std::size_t resumed = 0;
  std::size_t completion_resumed = 0;
  while (resumed < kMaxReadyWorkPerTurn) {
    const bool completion_available = !completion_ready_.Empty();
    const bool ready_available = !ready_.Empty();
    if (!completion_available && !ready_available) {
      break;
    }

    const bool run_completion =
        completion_available &&
        (!ready_available || completion_resumed < kMaxCompletionWorkPerTurn);
    coro::Work* work = nullptr;
    if (run_completion) {
      work = completion_ready_.PopFront();
      ++completion_resumed;
    } else {
      work = ready_.PopFront();
    }
    RunInExecutionScopeUnchecked(work);
    ++resumed;
  }
}

Result<void> Loop::FlushSubmit() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::FlushSubmit called from wrong thread");

  while (pending_submit_ > 0) {
    auto submitted = ring_.Submit();
    if (!submitted.HasValue()) {
      return std::unexpected(submitted.Error());
    }
    if (*submitted == 0) {
      return std::unexpected(Errno(EAGAIN));
    }

    const std::size_t count = std::min(*submitted, pending_submit_);
    pending_submit_ -= count;
    inflight_ += count;
    if (wake_pending_ && count > 0) {
      wake_pending_ = false;
      wake_inflight_ = true;
    }
  }

  return {};
}

Result<std::size_t> Loop::PollCompletions() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::PollCompletions called from wrong thread");

  auto flushed = FlushSubmit();
  if (!flushed.HasValue()) {
    return std::unexpected(flushed.Error());
  }

  return ring_.Reap(
      [this](io_uring_cqe* cqe) {
        HandleCqe(cqe);
        OnCqeHandled();
      },
      kMaxCqesPerTurn);
}

Result<std::size_t> Loop::WaitCompletions() noexcept {
  return WaitCompletionsFor(std::chrono::nanoseconds::max());
}

Result<std::size_t> Loop::WaitCompletionsFor(std::chrono::nanoseconds timeout) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::WaitCompletionsFor called from wrong thread");

  auto flushed = FlushSubmit();
  if (!flushed.HasValue()) {
    return std::unexpected(flushed.Error());
  }

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

  return ring_.Reap(
      [this](io_uring_cqe* completed_cqe) {
        HandleCqe(completed_cqe);
        OnCqeHandled();
      },
      kMaxCqesPerTurn);
}

void Loop::HandleCqe(io_uring_cqe* cqe) noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::HandleCqe called from wrong thread");

  auto* op = DecodeOp(cqe);
  if (op == nullptr) {
    if (inflight_ > 0) {
      --inflight_;
    }
    return;
  }

  const CompletionEvent event{cqe->res, cqe->flags};

  const auto apply_disposition = [this, op](CompletionDisposition disposition) noexcept {
    if (disposition.kernel_request_terminal) {
      ALYRN_CHECK(disposition.decrement_inflight,
                  "terminal Uring completion must decrement inflight work");
    }
    if (disposition.decrement_inflight) {
      ALYRN_CHECK(inflight_ > 0, "Loop inflight count underflow");
      --inflight_;
    }
    if (disposition.resume_continuation) {
      if (UsesCoupledSingleResultLifecycle(op->DispatchKind())) {
        ALYRN_CHECK(op->TryAuthorizeCoupledContinuation(),
                    "coupled Uring operation resumed before release authorization");
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
      if (!armed.HasValue()) {
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

  if (CompletionModelFor(op->DispatchKind()) == CompletionModel::kSingleShot &&
      !op->TryRecordCqeCompletion(cqe->res)) {
    return;
  }

  // For coupled single-shot awaiters, TryRecordCqeCompletion() settles the
  // physical CQE before DispatchCompletion() runs. Read/write CQEs directly
  // authorize result readiness; stream-valued Accept and Connect first refine
  // the CQE into Result<Stream>. Their adapter then crosses its backend-owned
  // release boundary; only apply_disposition() can authorize continuation
  // resumption. This is the uring refinement of:
  //
  //   result ready -> release -> continuation resume
  //
  // Composite and split-release operations return an explicit disposition and
  // retain their own stricter lifecycle state machines.
  apply_disposition(detail::DispatchCompletion(op, event));
}

Result<void> Loop::ArmWakePoll() noexcept {
  ALYRN_CHECK(IsInLoopThread(), "Loop::ArmWakePoll called from wrong thread");
  if (wake_fd_ < 0) {
    return std::unexpected(Errno(EBADF));
  }

  wake_op_.kind = OpKind::kWake;
  wake_op_.resume_work.ClearHandle();
  auto submitted = SubmitOp(&wake_op_, detail::PreparePollAdd(wake_fd_, POLLIN));
  if (submitted.HasValue()) {
    wake_pending_ = true;
  }
  return submitted;
}

void Loop::DrainWakeFd() noexcept {
  if (wake_fd_ < 0) {
    return;
  }

  std::uint64_t value = 0;
  while (::read(wake_fd_, &value, sizeof(value)) == sizeof(value)) {
  }
}

void Loop::Wake() noexcept {
  if (wake_fd_ < 0) {
    return;
  }

  constexpr std::uint64_t kWakeValue = 1;
  const ssize_t written = ::write(wake_fd_, &kWakeValue, sizeof(kWakeValue));
  if (written < 0 && errno != EAGAIN && errno != EINTR) {
    RequestStop();
  }
}

}  // namespace alyrn::uring
