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
#include "coropact/luring/op.h"
#include "coropact/luring/options.h"
#include "coropact/luring/ring.h"
#include "coropact/time/clock.h"

namespace coropact::luring {

namespace {

constexpr std::chrono::milliseconds kStopPollInterval{100};

[[nodiscard]]
LUringOp* DecodeOp(io_uring_cqe* cqe) noexcept {
  return reinterpret_cast<LUringOp*>(io_uring_cqe_get_data(cqe));
}

}  // namespace

namespace detail {

void DispatchCompletion(LUringOp* op, CompletionEvent event) noexcept {
  assert(op != nullptr);

  switch (op->DispatchKind()) {
    case LUringOpKind::kAcceptComplete:
      DispatchAcceptComplete(op);
      return;
    case LUringOpKind::kListenerCloseComplete:
      DispatchListenerCloseComplete(op);
      return;
    case LUringOpKind::kReadComplete:
      DispatchStreamReadComplete(op);
      return;
    case LUringOpKind::kTimedReadComplete:
      DispatchTimedReadComplete(op);
      return;
    case LUringOpKind::kTimedReadTimeoutComplete:
      DispatchTimedReadTimeoutComplete(op);
      return;
    case LUringOpKind::kWriteComplete:
      DispatchStreamWriteComplete(op);
      return;
    case LUringOpKind::kWritePartsComplete:
      DispatchStreamWritePartsComplete(op);
      return;
    case LUringOpKind::kStreamCloseComplete:
      DispatchStreamCloseComplete(op);
      return;
    case LUringOpKind::kTimerDriverComplete:
      DispatchTimerDriverComplete(op);
      return;
    case LUringOpKind::kTimerControlComplete:
      DispatchTimerControlComplete(op);
      return;
    case LUringOpKind::kAcceptSourceComplete:
      DispatchAcceptSourceComplete(op, event);
      return;
    case LUringOpKind::kAcceptSourceCancelComplete:
      DispatchAcceptSourceCancelComplete(op);
      return;
    case LUringOpKind::kRecvSourceComplete:
      DispatchRecvSourceComplete(op, event);
      return;
    case LUringOpKind::kSendZeroCopyComplete:
      DispatchSendZeroCopyComplete(op, event);
      return;
    case LUringOpKind::kRecvSourceCancelComplete:
      DispatchRecvSourceCancelComplete(op);
      return;
    case LUringOpKind::kNone:
    case LUringOpKind::kConnect:
    case LUringOpKind::kMsgRing:
    case LUringOpKind::kWake:
    case LUringOpKind::kCancelAll:
    case LUringOpKind::kNop:
      return;
    case LUringOpKind::kCount:
      break;
  }

  assert(false && "invalid LUring completion dispatch id");
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
  cancel_all_op_.ResetCompletion();
  cancel_all_op_.result = {};
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

  cancel_all_op_.ResetCompletion();
  cancel_all_op_.result = {};
  cancel_all_op_.resume_work.ClearHandle();

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

void LUringLoop::RunUntilIdle() {
  assert(IsInLoopThread());

  if (!initialized_) {
    return;
  }

  while (HasReadyWork() || PendingSubmitCount() > 0 || InflightCount() > 0) {
    RunReady();

    if (PendingSubmitCount() == 0 && InflightCount() == 0) {
      continue;
    }

    auto completed = WaitCompletions();
    if (!completed.has_value()) {
      break;
    }
  }

  RunReady();
}

base::Result<void> LUringLoop::FlushSubmit() noexcept {
  assert(IsInLoopThread());

  while (pending_submit_ > 0) {
    const std::size_t submitted = COROPACT_TRY(ring_.Submit());
    if (submitted == 0) {
      return std::unexpected(base::MakeErrno(EAGAIN));
    }

    const std::size_t n = std::min(submitted, pending_submit_);
    pending_submit_ -= n;
    inflight_ += n;
    if (wake_pending_ && n > 0) {
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

  const auto kind = op->DispatchKind();
  const bool is_multishot =
      kind == LUringOpKind::kAcceptSourceComplete ||
      kind == LUringOpKind::kRecvSourceComplete;
  const bool is_split_release = kind == LUringOpKind::kSendZeroCopyComplete;
  const CompletionEvent event{cqe->res, cqe->flags};
  const bool request_still_active = event.More();

  // F_MORE CQEs belong to the same physical request and keep one inflight
  // slot. Only the terminal CQE releases it.
  if ((!is_multishot && !is_split_release) ||
      (is_split_release && (!request_still_active || event.Notification())) ||
      (is_multishot && !request_still_active)) {
    assert(inflight_ > 0);
    if (inflight_ > 0) {
      --inflight_;
    }
  }

  if (op == &wake_op_) {
    wake_inflight_ = false;
    DrainWakeFd();
    if (!quit_.load(std::memory_order_acquire)) {
      wake_op_.ResetCompletion();
      wake_op_.result = {};
      auto armed = ArmWakePoll();
      if (!armed.has_value()) {
        quit_.store(true, std::memory_order_release);
      }
    }
    return;
  }

  if (op == &cancel_all_op_) {
    cancel_all_pending_ = false;
    static_cast<void>(op->Complete(cqe->res));
    return;
  }

  if (is_multishot && request_still_active) {
    detail::DispatchCompletion(op, event);
    return;
  }

  if (is_split_release) {
    detail::DispatchCompletion(op, event);
    const bool terminal = event.Notification() || !request_still_active;
    if (terminal && op->CompleteWithoutResult() && op->resume_work.HasHandle()) {
      ScheduleCompletion(&op->resume_work);
    }
    return;
  }

  const bool first_completion = op->Complete(cqe->res);
  if (first_completion) {
    detail::DispatchCompletion(op, event);
    if (op->resume_work.HasHandle()) {
      ScheduleCompletion(&op->resume_work);
    }
  }
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

  DrainMessages([this](const LUringMessage& message) noexcept {
    switch (message.type) {
      case LUringMessage::Type::kResume: {
        auto* work = reinterpret_cast<coro::Work*>(static_cast<std::uintptr_t>(message.data));
        if (work == nullptr) {
          assert(false && "mailbox resume message contains a null work pointer");
          return;
        }
        ScheduleCompletion(work);
        return;
      }
      case LUringMessage::Type::kFunction:
        assert(false && "mailbox function messages are not implemented");
        return;
    }

    assert(false && "unknown mailbox message type");
  });
}

}  // namespace coropact::luring
