// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include "coropact/luring/loop.h"

#include <liburing.h>
#include <liburing/io_uring.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <expected>
#include <stop_token>
#include <utility>

#include "coropact/base/current_thread.h"
#include "coropact/base/error.h"
#include "coropact/base/try.h"
#include "coropact/coro/scheduler.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/provided_buffer_pool.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/options.h"
#include "coropact/luring/detail/ring.h"
#include "coropact/time/clock.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

using namespace detail;

namespace {

constexpr std::chrono::milliseconds kStopPollInterval{100};

[[nodiscard]]
LUringOp* DecodeOp(io_uring_cqe* cqe) noexcept {
  return reinterpret_cast<LUringOp*>(io_uring_cqe_get_data(cqe));
}

}  // namespace

namespace detail {

CompletionDisposition DispatchCompletion(LUringOp* op, CompletionEvent event) noexcept {
  assert(op != nullptr);

  switch (op->DispatchKind()) {
    case LUringOpKind::kAcceptComplete:
      DispatchAcceptComplete(op);
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
    case LUringOpKind::kConnect:
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
    : Scheduler(frame_resource), thread_id_(base::tid()), timers_(this) {}

LUringLoop::~LUringLoop() noexcept {
  if (wake_fd_ >= 0) {
    ::close(wake_fd_);
  }
}

base::Result<void> LUringLoop::Init(const LUringOptions& options) noexcept {
  assert(IsInLoopThread());

  if (initialized_) {
    return std::unexpected(base::MakeErrno(EALREADY));
  }

  ring_ = COROPACT_TRY(LUringRing::Create(options));
  wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wake_fd_ < 0) {
    return std::unexpected(base::CurrentErrno());
  }
  submit_batch_ = options.submit_batch == 0 ? 1 : options.submit_batch;
  max_ready_work_per_turn_ = options.max_ready_work_per_turn;
  max_cqe_per_turn_ = options.max_cqe_per_turn;
  max_ready_time_per_turn_ = options.max_ready_time_per_turn;
  max_completion_work_per_turn_ = options.max_completion_work_per_turn;
  completion_queue_age_threshold_ =
      options.completion_queue_age_threshold > std::chrono::microseconds::zero()
          ? options.completion_queue_age_threshold
          : std::chrono::microseconds::zero();
  max_urgent_completion_work_per_turn_ = options.max_urgent_completion_work_per_turn;
  normal_queue_age_threshold_ =
      options.normal_queue_age_threshold > std::chrono::microseconds::zero()
          ? options.normal_queue_age_threshold
          : std::chrono::microseconds::zero();
  ready_depth_ = 0;
  completion_ready_depth_ = 0;
  ready_nonempty_since_ns_ = 0;
  completion_ready_nonempty_since_ns_ = 0;
  pending_submit_ = 0;
  inflight_ = 0;
  wake_pending_ = false;
  wake_inflight_ = false;
  cancel_all_pending_ = false;
  shared_buffer_pool_.reset();
  shared_buffer_capacity_ = options.shared_buffer_capacity;
  shared_buffer_size_ = options.shared_buffer_size;
  cancel_all_op_.BeginNextRequest();
  quit_.store(false, std::memory_order_relaxed);
  initialized_ = true;
  auto armed = ArmWakePoll();
  if (!armed.has_value()) {
    initialized_ = false;
    ::close(std::exchange(wake_fd_, -1));
    return std::unexpected(armed.error());
  }
  return {};
}

base::Result<detail::ProvidedBufferPool*> LUringLoop::GetSharedProvidedBufferPool(
    std::size_t buffer_size, std::size_t source_capacity) noexcept {
  assert(IsInLoopThread());
  if (shared_buffer_capacity_ == 0) {
    return std::unexpected(base::MakeErrno(ENOENT));
  }
  if (source_capacity == 0 || source_capacity > shared_buffer_capacity_) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (buffer_size != shared_buffer_size_) {
    return std::unexpected(base::MakeErrno(EINVAL));
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
    return std::unexpected(pool.error());
  }
  try {
    shared_buffer_pool_ = std::make_unique<detail::ProvidedBufferPool>(std::move(*pool));
  } catch (...) {
    return std::unexpected(base::MakeErrno(ENOMEM));
  }
  return shared_buffer_pool_.get();
}

void LUringLoop::Loop(std::stop_token token) noexcept {
  assert(IsInLoopThread());

  if (!initialized_) {
    return;
  }

  while (!token.stop_requested() && !quit_.load(std::memory_order_relaxed)) {
    // Observe already available completions before spending the turn on
    // ready work. This prevents a ready backlog from delaying CQE handling.
    auto completed = PollCompletions();
    if (!completed.has_value()) {
      break;
    }

    if (token.stop_requested() || quit_.load(std::memory_order_relaxed)) {
      break;
    }

    RunReady();

    if (*completed == 0 && !HasReadyWork() && inflight_ > 0) {
      completed = WaitCompletionsFor(kStopPollInterval);
      if (!completed.has_value() && completed.error().value() != ETIME) {
        break;
      }
    }
  }
}

void LUringLoop::Quit() noexcept {
  if (!quit_.exchange(true, std::memory_order_acq_rel)) {
    Wake();
  }
}

base::Result<void> LUringLoop::CancelPendingOperations() noexcept {
  assert(IsInLoopThread());

  if (cancel_all_pending_ || (PendingSubmitCount() == 0 && InflightCount() == 0)) {
    return {};
  }

  cancel_all_op_.BeginNextRequest();

  auto submitted = SubmitOp(&cancel_all_op_, [](io_uring_sqe* sqe) noexcept {
    io_uring_prep_cancel(sqe, nullptr, IORING_ASYNC_CANCEL_ANY | IORING_ASYNC_CANCEL_ALL);
  });
  if (submitted.has_value()) {
    cancel_all_pending_ = true;
  }
  return submitted;
}

void LUringLoop::Schedule(coro::Work* work) noexcept {
  assert(IsInLoopThread());
  if (ready_depth_ == 0) {
    ready_nonempty_since_ns_ = time::SteadyNowNs();
  }
  ++ready_depth_;
  ready_.PushBack(work);
}

void LUringLoop::ScheduleCompletion(coro::Work* work) noexcept {
  assert(IsInLoopThread());
  if (completion_ready_depth_ == 0) {
    completion_ready_nonempty_since_ns_ = time::SteadyNowNs();
  }
  ++completion_ready_depth_;
  completion_ready_.PushBack(work);
}

void LUringLoop::RunReady() noexcept {
  assert(IsInLoopThread());

  coro::Scheduler* previous = coro::Scheduler::Current();
  coro::Scheduler::SetCurrent(this);

  // The common throughput configuration does not need wall-clock fairness.
  // Avoid reading the clock for every resumed work item in that mode; the
  // budgeted path below keeps the fairness policy when timing control is on.
  const bool timing_required =
      max_ready_time_per_turn_ > std::chrono::microseconds::zero() ||
      completion_queue_age_threshold_ > std::chrono::microseconds::zero() ||
      normal_queue_age_threshold_ > std::chrono::microseconds::zero();
  if (!timing_required) {
    std::size_t resumed = 0;
    std::size_t completion_resumed = 0;
    while (HasReadyWork() &&
           (max_ready_work_per_turn_ == 0 || resumed < max_ready_work_per_turn_)) {
      coro::Work* work = nullptr;
      const bool run_completion =
          !completion_ready_.Empty() && (ready_.Empty() || max_completion_work_per_turn_ == 0 ||
                                         completion_resumed < max_completion_work_per_turn_);
      if (run_completion) {
        work = completion_ready_.PopFront();
        assert(completion_ready_depth_ > 0);
        --completion_ready_depth_;
        if (completion_ready_depth_ == 0) {
          completion_ready_nonempty_since_ns_ = 0;
        }
        ++completion_resumed;
      } else {
        work = ready_.PopFront();
        assert(ready_depth_ > 0);
        --ready_depth_;
        if (ready_depth_ == 0) {
          ready_nonempty_since_ns_ = 0;
        }
      }
      Run(work);
      ++resumed;
    }
    coro::Scheduler::SetCurrent(previous);
    return;
  }

  const std::uint64_t turn_start_ns = time::SteadyNowNs();
  const auto configured_time_budget = max_ready_time_per_turn_ > std::chrono::microseconds::zero()
                                          ? max_ready_time_per_turn_
                                          : std::chrono::microseconds::zero();
  const std::uint64_t time_budget_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(configured_time_budget).count());
  const auto configured_age_threshold =
      completion_queue_age_threshold_ > std::chrono::microseconds::zero()
          ? completion_queue_age_threshold_
          : std::chrono::microseconds::zero();
  const std::uint64_t completion_age_threshold_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(configured_age_threshold).count());
  const auto configured_normal_age_threshold =
      normal_queue_age_threshold_ > std::chrono::microseconds::zero()
          ? normal_queue_age_threshold_
          : std::chrono::microseconds::zero();
  const std::uint64_t normal_age_threshold_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(configured_normal_age_threshold)
          .count());
  const bool completion_is_urgent =
      completion_ready_depth_ > 0 && completion_age_threshold_ns != 0 &&
      turn_start_ns - completion_ready_nonempty_since_ns_ >= completion_age_threshold_ns;
  const bool normal_is_overdue =
      ready_depth_ > 0 && normal_age_threshold_ns != 0 &&
      turn_start_ns - ready_nonempty_since_ns_ >= normal_age_threshold_ns;
  const bool use_urgent_completion_budget = completion_is_urgent && !normal_is_overdue;
  const std::size_t completion_budget =
      use_urgent_completion_budget && max_urgent_completion_work_per_turn_ != 0
          ? max_urgent_completion_work_per_turn_
          : max_completion_work_per_turn_;

  std::size_t resumed = 0;
  std::size_t completion_resumed = 0;
  while (HasReadyWork() && (max_ready_work_per_turn_ == 0 || resumed < max_ready_work_per_turn_)) {
    coro::Work* work = nullptr;
    const bool run_completion =
        !completion_ready_.Empty() &&
        (ready_.Empty() || (!normal_is_overdue &&
                            (completion_budget == 0 || completion_resumed < completion_budget)));
    if (run_completion) {
      work = completion_ready_.PopFront();
      assert(completion_ready_depth_ > 0);
      --completion_ready_depth_;
      if (completion_ready_depth_ == 0) {
        completion_ready_nonempty_since_ns_ = 0;
      }
      ++completion_resumed;
    } else {
      work = ready_.PopFront();
      assert(ready_depth_ > 0);
      --ready_depth_;
      if (ready_depth_ == 0) {
        ready_nonempty_since_ns_ = 0;
      }
    }
    Run(work);
    ++resumed;

    if (time_budget_ns != 0 && time::SteadyNowNs() - turn_start_ns >= time_budget_ns) {
      break;
    }
  }

  coro::Scheduler::SetCurrent(previous);
}

base::Result<void> LUringLoop::FlushSubmit() noexcept {
  assert(IsInLoopThread());

  while (pending_submit_ > 0) {
    const std::size_t submitted = COROPACT_TRY(ring_.Submit());
    if (submitted == 0) {
      return std::unexpected(base::MakeErrno(EAGAIN));
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

base::Result<std::size_t> LUringLoop::PollCompletions() noexcept {
  assert(IsInLoopThread());

  COROPACT_TRY(FlushSubmit());

  return ring_.Reap([this](io_uring_cqe* cqe) { HandleCqe(cqe); }, max_cqe_per_turn_);
}

base::Result<std::size_t> LUringLoop::WaitCompletions() noexcept {
  return WaitCompletionsFor(std::chrono::nanoseconds::max());
}

base::Result<std::size_t> LUringLoop::WaitCompletionsFor(
    std::chrono::nanoseconds timeout) noexcept {
  assert(IsInLoopThread());

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
    return std::unexpected(base::MakeNegErrno(r));
  }

  return ring_.Reap([this](io_uring_cqe* completed_cqe) { HandleCqe(completed_cqe); },
                    max_cqe_per_turn_);
}

void LUringLoop::HandleCqe(io_uring_cqe* cqe) noexcept {
  assert(IsInLoopThread());

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
      assert(disposition.decrement_inflight);
    }
    if (disposition.decrement_inflight) {
      assert(inflight_ > 0);
      if (inflight_ > 0) {
        --inflight_;
      }
    }
    if (disposition.resume_continuation) {
      const bool logical_completion_ready = op->IsCompleted() || op->CompleteWithoutResult();
      if (logical_completion_ready && op->resume_work.HasHandle()) {
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
    if (!quit_.load(std::memory_order_acquire)) {
      wake_op_.BeginNextRequest();
      auto armed = ArmWakePoll();
      if (!armed.has_value()) {
        quit_.store(true, std::memory_order_release);
      }
    }
    return;
  }

  if (op == &cancel_all_op_) {
    cancel_all_pending_ = false;
    COROPACT_IGNORE_RESULT(op->Complete(cqe->res));
    apply_disposition(detail::DispatchCompletion(op, event));
    return;
  }

  if (CompletionModelFor(op->DispatchKind()) == LUringCompletionModel::kSingleShot &&
      !op->Complete(cqe->res)) {
    return;
  }

  apply_disposition(detail::DispatchCompletion(op, event));
}

base::Result<void> LUringLoop::ArmWakePoll() noexcept {
  assert(IsInLoopThread());
  if (wake_fd_ < 0) {
    return std::unexpected(base::MakeErrno(EBADF));
  }

  wake_op_.kind = LUringOpKind::kWake;
  wake_op_.resume_work.ClearHandle();
  auto submitted = SubmitOp(&wake_op_, [fd = wake_fd_](io_uring_sqe* sqe) noexcept {
    io_uring_prep_poll_add(sqe, fd, POLLIN);
  });
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
    quit_.store(true, std::memory_order_release);
  }
}

void LUringLoop::HandleMailbox() noexcept {
  assert(IsInLoopThread());

  mailbox_.Drain([this](const LUringMessage& message) noexcept {
    auto* work = reinterpret_cast<coro::Work*>(static_cast<std::uintptr_t>(message.data));
    if (work == nullptr) {
      assert(false && "mailbox message contains a null work pointer");
      return;
    }
    ScheduleCompletion(work);
  });
}

}  // namespace coropact::luring
