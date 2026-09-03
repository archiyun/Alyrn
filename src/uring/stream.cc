// SPDX-License-Identifier: MIT
#include "alyrn/uring/stream.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <cstddef>
#include <cstring>
#include <expected>
#include <new>
#include <span>
#include <utility>

#include "alyrn/backend/loop.h"
#include "alyrn/detail/check.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/recv.h"
#include "alyrn/result.h"
#include "alyrn/time/clock.h"
#include "alyrn/uring/detail/fd_close_convergence.h"
#include "alyrn/uring/detail/loop_access.h"
#include "alyrn/uring/detail/op.h"
#include "alyrn/uring/detail/op_hook.h"
#include "alyrn/uring/detail/operation_submission.h"
#include "alyrn/uring/detail/provided_buffer_pool.h"
#include "alyrn/uring/detail/sqe_prep.h"
#include "alyrn/uring/detail/stream_operation_slot.h"
#include "alyrn/uring/loop.h"

namespace alyrn::uring {

using namespace detail;

namespace {

constexpr std::size_t kRecvMaxIov = 16;

Result<std::size_t> ToSizeResult(const CqeResult& result, bool timed_out) noexcept {
  ALYRN_CHECK(result.HasValue(), "Uring stream awaiter resumed before its CQE result was ready");
  const int cqe_result = *result;
  if (timed_out) {
    return std::unexpected(Errno(ETIMEDOUT));
  }
  if (cqe_result < 0) {
    return std::unexpected(NegErrno(cqe_result));
  }
  return static_cast<std::size_t>(cqe_result);
}

}  // namespace

CompletionDisposition Stream::DeadlineCancelOp::OnComplete(Op* op) noexcept {
  auto* self = detail::OpHook<DeadlineCancelOp>::OwnerFrom(op);
  return self->stream_->CompleteDeadlineCancel(self->read_);
}

Result<void> Stream::BeginReadOperation(Op* operation, void* owner,
                                        DeadlineFinalizer finalizer) noexcept {
  ALYRN_CHECK(operation != nullptr, "Uring read deadline requires an operation");
  ALYRN_CHECK(owner != nullptr, "Uring read deadline requires an owner");
  ALYRN_CHECK(finalizer != nullptr, "Uring read deadline requires a finalizer");
  ALYRN_CHECK(read_deadline_target_ == nullptr,
              "Uring read deadline state was not released before a new operation");

  read_cancel_op_.BeginNextRequest();
  read_deadline_target_ = operation;
  read_deadline_owner_ = owner;
  read_deadline_finalizer_ = finalizer;
  read_cancel_requested_ = false;
  read_cancel_submitted_ = false;
  read_cancel_terminal_ = false;
  read_primary_terminal_ = false;
  read_timed_out_ = false;

  if (read_deadline_.has_value() && *read_deadline_ <= time::SteadyNow()) {
    return std::unexpected(Errno(ETIMEDOUT));
  }

  if (read_deadline_.has_value() && !ArmReadDeadlineTimer()) {
    return std::unexpected(Errno(ENOMEM));
  }
  return {};
}

Result<void> Stream::BeginWriteOperation(Op* operation, void* owner,
                                         DeadlineFinalizer finalizer) noexcept {
  ALYRN_CHECK(operation != nullptr, "Uring write deadline requires an operation");
  ALYRN_CHECK(owner != nullptr, "Uring write deadline requires an owner");
  ALYRN_CHECK(finalizer != nullptr, "Uring write deadline requires a finalizer");
  ALYRN_CHECK(write_deadline_target_ == nullptr,
              "Uring write deadline state was not released before a new operation");

  write_cancel_op_.BeginNextRequest();
  write_deadline_target_ = operation;
  write_deadline_owner_ = owner;
  write_deadline_finalizer_ = finalizer;
  write_cancel_requested_ = false;
  write_cancel_submitted_ = false;
  write_cancel_terminal_ = false;
  write_primary_terminal_ = false;
  write_timed_out_ = false;

  if (write_deadline_.has_value() && *write_deadline_ <= time::SteadyNow()) {
    return std::unexpected(Errno(ETIMEDOUT));
  }

  if (write_deadline_.has_value() && !ArmWriteDeadlineTimer()) {
    return std::unexpected(Errno(ENOMEM));
  }
  return {};
}

void Stream::AbortReadOperation(Op* operation) noexcept {
  ALYRN_CHECK(read_deadline_target_ == operation,
              "Uring read deadline aborted for a different operation");
  CancelReadDeadlineTimer();
  read_deadline_target_ = nullptr;
  read_deadline_owner_ = nullptr;
  read_deadline_finalizer_ = nullptr;
  read_cancel_requested_ = false;
  read_cancel_submitted_ = false;
  read_cancel_terminal_ = false;
  read_primary_terminal_ = false;
  read_timed_out_ = false;
}

void Stream::AbortWriteOperation(Op* operation) noexcept {
  ALYRN_CHECK(write_deadline_target_ == operation,
              "Uring write deadline aborted for a different operation");
  CancelWriteDeadlineTimer();
  write_deadline_target_ = nullptr;
  write_deadline_owner_ = nullptr;
  write_deadline_finalizer_ = nullptr;
  write_cancel_requested_ = false;
  write_cancel_submitted_ = false;
  write_cancel_terminal_ = false;
  write_primary_terminal_ = false;
  write_timed_out_ = false;
}

void Stream::CompleteReadOperation(Op* operation) noexcept {
  ALYRN_CHECK(read_deadline_target_ == operation,
              "Uring read completion belongs to a different deadline state");
  read_primary_terminal_ = true;
  CancelReadDeadlineTimer();

  if (read_cancel_submitted_ && !read_cancel_terminal_) {
    if (operation->resume_work.HasHandle()) {
      read_cancel_op_.resume_work.SetHandle(operation->resume_work.Handle());
      operation->resume_work.ClearHandle();
    }
    return;
  }
  FinalizeReadOperation();
}

void Stream::CompleteWriteOperation(Op* operation) noexcept {
  ALYRN_CHECK(write_deadline_target_ == operation,
              "Uring write completion belongs to a different deadline state");
  write_primary_terminal_ = true;
  CancelWriteDeadlineTimer();

  if (write_cancel_submitted_ && !write_cancel_terminal_) {
    if (operation->resume_work.HasHandle()) {
      write_cancel_op_.resume_work.SetHandle(operation->resume_work.Handle());
      operation->resume_work.ClearHandle();
    }
    return;
  }
  FinalizeWriteOperation();
}

CompletionDisposition Stream::CompleteDeadlineCancel(bool read) noexcept {
  Op& cancel_op = read ? read_cancel_op_ : write_cancel_op_;
  bool& cancel_submitted = read ? read_cancel_submitted_ : write_cancel_submitted_;
  bool& cancel_terminal = read ? read_cancel_terminal_ : write_cancel_terminal_;
  bool& primary_terminal = read ? read_primary_terminal_ : write_primary_terminal_;

  ALYRN_CHECK(cancel_submitted, "Uring deadline cancel completed before submission");
  cancel_terminal = true;
  if (!primary_terminal) {
    return CompletionDisposition{
        .kernel_request_terminal = true,
        .decrement_inflight = true,
        .resume_continuation = false,
    };
  }

  const bool resume_continuation = cancel_op.resume_work.HasHandle();
  if (read) {
    FinalizeReadOperation();
  } else {
    FinalizeWriteOperation();
  }
  return CompletionDisposition{
      .kernel_request_terminal = true,
      .decrement_inflight = true,
      .resume_continuation = resume_continuation,
  };
}

void Stream::FinalizeReadOperation() noexcept {
  ALYRN_CHECK(read_deadline_target_ != nullptr,
              "Uring read finalized without a registered operation");
  ALYRN_CHECK(read_primary_terminal_, "Uring read finalized before its primary CQE");
  ALYRN_CHECK(!read_cancel_submitted_ || read_cancel_terminal_,
              "Uring read finalized before its deadline cancel CQE");

  auto* owner = read_deadline_owner_;
  const auto finalizer = read_deadline_finalizer_;
  finalizer(owner);
  CancelReadDeadlineTimer();
  read_deadline_target_ = nullptr;
  read_deadline_owner_ = nullptr;
  read_deadline_finalizer_ = nullptr;
  read_cancel_requested_ = false;
  read_cancel_submitted_ = false;
  read_cancel_terminal_ = false;
  read_primary_terminal_ = false;
  read_timed_out_ = false;
}

void Stream::FinalizeWriteOperation() noexcept {
  ALYRN_CHECK(write_deadline_target_ != nullptr,
              "Uring write finalized without a registered operation");
  ALYRN_CHECK(write_primary_terminal_, "Uring write finalized before its primary CQE");
  ALYRN_CHECK(!write_cancel_submitted_ || write_cancel_terminal_,
              "Uring write finalized before its deadline cancel CQE");

  auto* owner = write_deadline_owner_;
  const auto finalizer = write_deadline_finalizer_;
  finalizer(owner);
  CancelWriteDeadlineTimer();
  write_deadline_target_ = nullptr;
  write_deadline_owner_ = nullptr;
  write_deadline_finalizer_ = nullptr;
  write_cancel_requested_ = false;
  write_cancel_submitted_ = false;
  write_cancel_terminal_ = false;
  write_primary_terminal_ = false;
  write_timed_out_ = false;
}

Result<void> Stream::SubmitReadDeadlineCancel() noexcept {
  ALYRN_CHECK(read_deadline_target_ != nullptr,
              "Uring read deadline cancel has no target operation");
  if (read_cancel_submitted_) {
    return {};
  }

  read_cancel_op_.BeginNextRequest();
  auto submitted = LoopAccess::SubmitOp(
      *loop_, read_cancel_op_.Operation(),
      PrepareCancelAllByUserData(reinterpret_cast<std::uint64_t>(read_deadline_target_)));
  if (!submitted.has_value()) {
    return std::unexpected(submitted.error());
  }
  read_cancel_submitted_ = true;
  return {};
}

Result<void> Stream::SubmitWriteDeadlineCancel() noexcept {
  ALYRN_CHECK(write_deadline_target_ != nullptr,
              "Uring write deadline cancel has no target operation");
  if (write_cancel_submitted_) {
    return {};
  }

  write_cancel_op_.BeginNextRequest();
  auto submitted = LoopAccess::SubmitOp(
      *loop_, write_cancel_op_.Operation(),
      PrepareCancelAllByUserData(reinterpret_cast<std::uint64_t>(write_deadline_target_)));
  if (!submitted.has_value()) {
    return std::unexpected(submitted.error());
  }
  write_cancel_submitted_ = true;
  return {};
}

bool Stream::ScheduleReadDeadlineCancelRetry(std::uint64_t generation) noexcept {
  try {
    auto retry = loop_->RunAfter(time::Milliseconds(1),
                                 [this, generation] { RetryReadDeadlineCancel(generation); });
    if (!retry.has_value()) {
      return false;
    }
    read_timer_ = *retry;
    return true;
  } catch (...) {
    return false;
  }
}

bool Stream::ScheduleWriteDeadlineCancelRetry(std::uint64_t generation) noexcept {
  try {
    auto retry = loop_->RunAfter(time::Milliseconds(1),
                                 [this, generation] { RetryWriteDeadlineCancel(generation); });
    if (!retry.has_value()) {
      return false;
    }
    write_timer_ = *retry;
    return true;
  } catch (...) {
    return false;
  }
}

void Stream::HandleReadDeadline(std::uint64_t generation) noexcept {
  if (generation != read_timer_generation_) {
    return;
  }
  read_timer_ = {};
  if (read_deadline_target_ == nullptr || read_primary_terminal_ || read_cancel_submitted_) {
    return;
  }

  read_timed_out_ = true;
  read_cancel_requested_ = true;
  auto cancelled = SubmitReadDeadlineCancel();
  if (!cancelled.has_value() && !ScheduleReadDeadlineCancelRetry(generation)) {
    loop_->RequestStop();
  }
}

void Stream::HandleWriteDeadline(std::uint64_t generation) noexcept {
  if (generation != write_timer_generation_) {
    return;
  }
  write_timer_ = {};
  if (write_deadline_target_ == nullptr || write_primary_terminal_ || write_cancel_submitted_) {
    return;
  }

  write_timed_out_ = true;
  write_cancel_requested_ = true;
  auto cancelled = SubmitWriteDeadlineCancel();
  if (!cancelled.has_value() && !ScheduleWriteDeadlineCancelRetry(generation)) {
    loop_->RequestStop();
  }
}

void Stream::RetryReadDeadlineCancel(std::uint64_t generation) noexcept {
  if (generation != read_timer_generation_) {
    return;
  }
  read_timer_ = {};
  if (read_deadline_target_ == nullptr || read_primary_terminal_ || read_cancel_submitted_) {
    return;
  }

  auto cancelled = SubmitReadDeadlineCancel();
  if (!cancelled.has_value() && !ScheduleReadDeadlineCancelRetry(generation)) {
    loop_->RequestStop();
  }
}

void Stream::RetryWriteDeadlineCancel(std::uint64_t generation) noexcept {
  if (generation != write_timer_generation_) {
    return;
  }
  write_timer_ = {};
  if (write_deadline_target_ == nullptr || write_primary_terminal_ || write_cancel_submitted_) {
    return;
  }

  auto cancelled = SubmitWriteDeadlineCancel();
  if (!cancelled.has_value() && !ScheduleWriteDeadlineCancelRetry(generation)) {
    loop_->RequestStop();
  }
}

bool Stream::ArmReadDeadlineTimer() noexcept {
  if (read_deadline_target_ == nullptr || !read_deadline_.has_value() || read_cancel_requested_) {
    return true;
  }

  const std::uint64_t generation = ++read_timer_generation_;
  const auto now = time::SteadyNow();
  const auto delay = *read_deadline_ > now ? *read_deadline_ - now : time::Duration::zero();
  try {
    auto timer = loop_->RunAfter(delay, [this, generation] { HandleReadDeadline(generation); });
    if (!timer.has_value()) {
      return false;
    }
    read_timer_ = *timer;
    return true;
  } catch (...) {
    return false;
  }
}

bool Stream::ArmWriteDeadlineTimer() noexcept {
  if (write_deadline_target_ == nullptr || !write_deadline_.has_value() ||
      write_cancel_requested_) {
    return true;
  }

  const std::uint64_t generation = ++write_timer_generation_;
  const auto now = time::SteadyNow();
  const auto delay = *write_deadline_ > now ? *write_deadline_ - now : time::Duration::zero();
  try {
    auto timer = loop_->RunAfter(delay, [this, generation] { HandleWriteDeadline(generation); });
    if (!timer.has_value()) {
      return false;
    }
    write_timer_ = *timer;
    return true;
  } catch (...) {
    return false;
  }
}

void Stream::CancelReadDeadlineTimer() noexcept {
  ++read_timer_generation_;
  const auto timer = std::exchange(read_timer_, time::TimerId{});
  if (timer.Valid()) {
    (void)loop_->CancelTimer(timer);
  }
}

void Stream::CancelWriteDeadlineTimer() noexcept {
  ++write_timer_generation_;
  const auto timer = std::exchange(write_timer_, time::TimerId{});
  if (timer.Valid()) {
    (void)loop_->CancelTimer(timer);
  }
}
Result<void> detail::StreamOperationSlot::Validate(Stream& stream,
                                                   StreamOperationDirection direction) noexcept {
  stream.RequireOwnerLoop();

  const backend::LoopState loop_state = stream.loop_->State();
  if (loop_state == backend::LoopState::kStopping || loop_state == backend::LoopState::kStopped) {
    return std::unexpected(Errno(ECANCELED));
  }

  auto valid = direction == StreamOperationDirection::kRead ? stream.lifecycle_.ValidateRead()
                                                            : stream.lifecycle_.ValidateWrite();
  if (!valid.has_value()) {
    return valid;
  }
  if (stream.fd_ < 0) {
    return std::unexpected(Errno(EBADF));
  }
  return {};
}

Result<void> detail::StreamOperationSlot::Reserve(Stream& stream,
                                                  StreamOperationDirection direction,
                                                  void* operation) noexcept {
  auto available = ValidateAvailable(stream, direction);
  if (!available.has_value()) {
    return available;
  }

  void*& pending =
      direction == StreamOperationDirection::kRead ? stream.pending_read_ : stream.pending_write_;
  pending = operation;
  return {};
}

Result<void> detail::StreamOperationSlot::ValidateAvailable(
    Stream& stream, StreamOperationDirection direction) noexcept {
  auto valid = Validate(stream, direction);
  if (!valid.has_value()) {
    return valid;
  }

  void* pending =
      direction == StreamOperationDirection::kRead ? stream.pending_read_ : stream.pending_write_;
  if (pending != nullptr) {
    return std::unexpected(Errno(EBUSY));
  }
  return {};
}

void detail::StreamOperationSlot::Release(Stream& stream, StreamOperationDirection direction,
                                          void* operation) noexcept {
  void*& pending =
      direction == StreamOperationDirection::kRead ? stream.pending_read_ : stream.pending_write_;
  if (pending == operation) {
    pending = nullptr;
    stream.NotifyCloseProgress();
  }
}

// ---- ReadAwaiter ---
bool Stream::ReadAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  auto available = detail::StreamOperationSlot::ValidateAvailable(
      *stream_, detail::StreamOperationDirection::kRead);
  if (!available.has_value()) {
    Operation()->SetImmediateError(available.error());
    return false;
  }
  if (stream_->lifecycle_.IsReadShutdown()) {
    Operation()->SetImmediateSuccess();
    return false;
  }
  auto reserved =
      detail::StreamOperationSlot::Reserve(*stream_, detail::StreamOperationDirection::kRead, this);
  if (!reserved.has_value()) {
    Operation()->SetImmediateError(reserved.error());
    return false;
  }
  if (buffer_.empty()) {
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
    Operation()->SetImmediateSuccess();
    return false;
  }

  Operation()->kind = OpKind::kReadComplete;
  auto begun = stream_->BeginReadOperation(Operation(), this, &ReadAwaiter::Finalize);
  if (!begun.has_value()) {
    stream_->AbortReadOperation(Operation());
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
    Operation()->SetImmediateError(begun.error());
    return false;
  }
  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Operation(), continuation,
      detail::PrepareRecv(stream_->fd_, buffer_.data(), buffer_.size()),
      [this](Error error) noexcept {
        stream_->AbortReadOperation(Operation());
        detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead,
                                             this);
        Operation()->SetImmediateError(error);
      });
}

Result<std::size_t> Stream::ReadAwaiter::await_resume() noexcept {
  return ToSizeResult(Operation()->result, timed_out_);
}

void Stream::ReadAwaiter::OnComplete(::alyrn::uring::detail::Op* op) noexcept {
  auto* self = OpHook::OwnerFrom(op);
  self->timed_out_ = self->stream_->ReadDeadlineTimedOut();
  self->stream_->CompleteReadOperation(op);
}

void Stream::ReadAwaiter::Finalize(void* owner) noexcept {
  auto* self = static_cast<ReadAwaiter*>(owner);
  ALYRN_CHECK(self->Operation()->TryAuthorizeCoupledRelease(),
              "Uring Read released its stream slot before result readiness");
  detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kRead,
                                       self);
}

// ---- RecvAwaiter ---
bool Stream::RecvAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  auto available = detail::StreamOperationSlot::ValidateAvailable(
      *stream_, detail::StreamOperationDirection::kRead);
  if (!available.has_value()) {
    Operation()->SetImmediateError(available.error());
    return false;
  }
  if (stream_->lifecycle_.IsReadShutdown()) {
    Operation()->SetImmediateSuccess();
    return false;
  }
  auto reserved =
      detail::StreamOperationSlot::Reserve(*stream_, detail::StreamOperationDirection::kRead, this);
  if (!reserved.has_value()) {
    Operation()->SetImmediateError(reserved.error());
    return false;
  }
  iovec single_iov{};
  const ReservationKind reservation = PrepareReservation(single_iov);
  if (reservation == ReservationKind::kNone) {
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
    Operation()->SetImmediateError(Errno(ENOMEM));
    return false;
  }

  Operation()->kind = OpKind::kRecvComplete;

  auto begun = stream_->BeginReadOperation(Operation(), this, &RecvAwaiter::Finalize);
  if (!begun.has_value()) {
    stream_->AbortReadOperation(Operation());
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
    FinishReservation(std::unexpected(begun.error()));
    Operation()->SetImmediateError(begun.error());
    return false;
  }

  auto on_submit_failure = [this](Error error) noexcept {
    stream_->AbortReadOperation(Operation());
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
    FinishReservation(std::unexpected(error));
    Operation()->SetImmediateError(error);
  };

  if (reservation == ReservationKind::kSingle) {
    return detail::SubmitAwaitingOperation(
        *stream_->loop_, *Operation(), continuation,
        detail::PrepareRecv(stream_->fd_, single_iov.iov_base, single_iov.iov_len),
        std::move(on_submit_failure));
  }

  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Operation(), continuation,
      detail::PrepareReadv(stream_->fd_, iovs_.data(), static_cast<unsigned>(iovs_.size()), -1),
      std::move(on_submit_failure));
}

net::RecvOutcome Stream::RecvAwaiter::await_resume() noexcept {
  return {
      .result = ToSizeResult(Operation()->result, timed_out_),
      .buffer = std::move(buffer_),
  };
}

void Stream::RecvAwaiter::OnComplete(::alyrn::uring::detail::Op* op) noexcept {
  auto* self = OpHook::OwnerFrom(op);
  self->timed_out_ = self->stream_->ReadDeadlineTimedOut();
  self->FinishReservation(ToSizeResult(op->result, self->timed_out_));
  self->stream_->CompleteReadOperation(op);
}

void Stream::RecvAwaiter::Finalize(void* owner) noexcept {
  auto* self = static_cast<RecvAwaiter*>(owner);
  ALYRN_CHECK(self->Operation()->TryAuthorizeCoupledRelease(),
              "Uring Recv released its reservation before result readiness");
  detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kRead,
                                       self);
}

Stream::RecvAwaiter::ReservationKind Stream::RecvAwaiter::PrepareReservation(
    iovec& single_iov) noexcept {
  try {
    if (auto iov = buffer_.TryPrepareWriteOne(reserve_); iov.has_value()) {
      single_iov = *iov;
      reservation_kind_ = ReservationKind::kSingle;
    } else {
      auto iovs = buffer_.PrepareWrite(reserve_, kRecvMaxIov);
      if (iovs.empty()) {
        buffer_.AbortWrite();
        return ReservationKind::kNone;
      }
      iovs_ = std::move(iovs);
      reservation_kind_ = ReservationKind::kMultiple;
    }
  } catch (const std::bad_alloc&) {
    buffer_.AbortWrite();
    return ReservationKind::kNone;
  }
  return reservation_kind_;
}

void Stream::RecvAwaiter::FinishReservation(Result<std::size_t> result) noexcept {
  ALYRN_CHECK(reservation_kind_ != ReservationKind::kNone,
              "RecvAwaiter completion without a buffer reservation");
  if (result.has_value()) {
    buffer_.CommitWrite(*result);
  } else {
    buffer_.AbortWrite();
  }
  iovs_.clear();
  reservation_kind_ = ReservationKind::kNone;
}

// ---- RecvCopyAwaiter ---
bool Stream::RecvCopyAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  auto available = detail::StreamOperationSlot::ValidateAvailable(
      *stream_, detail::StreamOperationDirection::kRead);
  if (!available.has_value()) {
    Finish(std::unexpected(available.error()));
    Operation()->SetImmediateError(available.error());
    return false;
  }
  if (stream_->lifecycle_.IsReadShutdown()) {
    Finish(net::Buffer{});
    Operation()->SetImmediateSuccess();
    return false;
  }

  auto pool = detail::LoopAccess::GetSharedProvidedBufferPool(*stream_->loop_, 1);
  if (!pool.has_value()) {
    Finish(std::unexpected(pool.error()));
    Operation()->SetImmediateError(pool.error());
    return false;
  }
  pool_ = *pool;

  auto reserved =
      detail::StreamOperationSlot::Reserve(*stream_, detail::StreamOperationDirection::kRead, this);
  if (!reserved.has_value()) {
    Finish(std::unexpected(reserved.error()));
    Operation()->SetImmediateError(reserved.error());
    return false;
  }

  Operation()->kind = OpKind::kRecvCopyComplete;
  auto begun = stream_->BeginReadOperation(Operation(), this, &RecvCopyAwaiter::Finalize);
  if (!begun.has_value()) {
    stream_->AbortReadOperation(Operation());
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
    Finish(std::unexpected(begun.error()));
    Operation()->SetImmediateError(begun.error());
    return false;
  }
  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Operation(), continuation,
      detail::PrepareProvidedRecv(stream_->fd_, pool_->buffer_size(), pool_->BufferGroup()),
      [this](Error error) noexcept {
        stream_->AbortReadOperation(Operation());
        detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead,
                                             this);
        Finish(std::unexpected(error));
        Operation()->SetImmediateError(error);
      });
}

Result<net::Buffer> Stream::RecvCopyAwaiter::await_resume() noexcept {
  ALYRN_CHECK(outcome_.has_value(), "Uring Recv() resumed before its result was ready");
  return std::move(*outcome_);
}

void Stream::RecvCopyAwaiter::Finish(Result<net::Buffer> outcome) noexcept {
  ALYRN_CHECK(!outcome_.has_value(), "Uring Recv() recorded two results");
  outcome_ = std::move(outcome);
}

void Stream::RecvCopyAwaiter::OnComplete(::alyrn::uring::detail::Op* op,
                                         CompletionEvent event) noexcept {
  auto* self = OpHook::OwnerFrom(op);
  ALYRN_CHECK(op->TryAuthorizeCoupledResult(), "Uring Recv() result was authorized twice");
  self->timed_out_ = self->stream_->ReadDeadlineTimedOut();

  const int cqe_result = event.result;
  const bool has_buffer = event.HasSelectedBuffer();
  const auto buffer_id = event.SelectedBufferId();
  auto* pool = self->pool_;
  const bool valid_buffer = has_buffer && pool != nullptr && buffer_id < pool->capacity() &&
                            pool->slot(buffer_id) != nullptr;

  auto return_slot = [pool, valid_buffer, buffer_id]() noexcept {
    if (!valid_buffer) {
      return;
    }
    if (pool->Acquire(buffer_id)) {
      ALYRN_CHECK(pool->Return(buffer_id), "Uring Recv() failed to return provided buffer");
    }
  };

  if (self->timed_out_) {
    return_slot();
    self->Finish(std::unexpected(Errno(ETIMEDOUT)));
  } else if (cqe_result < 0) {
    return_slot();
    self->Finish(std::unexpected(NegErrno(cqe_result)));
  } else if (cqe_result == 0) {
    return_slot();
    self->Finish(net::Buffer{});
  } else if (!valid_buffer || event.BufferMore()) {
    return_slot();
    self->Finish(std::unexpected(Errno(EPROTO)));
  } else if (!pool->Acquire(buffer_id)) {
    self->Finish(std::unexpected(Errno(EPROTO)));
  } else {
    net::Buffer buffer;
    try {
      buffer.Append(
          std::span<const std::byte>(pool->slot(buffer_id), static_cast<std::size_t>(cqe_result)));
    } catch (const std::bad_alloc&) {
      ALYRN_CHECK(pool->Return(buffer_id), "Uring Recv() failed to return provided buffer");
      self->Finish(std::unexpected(Errno(ENOMEM)));
      self->stream_->CompleteReadOperation(op);
      return;
    }
    ALYRN_CHECK(pool->Return(buffer_id), "Uring Recv() failed to return provided buffer");
    self->Finish(std::move(buffer));
  }

  self->stream_->CompleteReadOperation(op);
}

void Stream::RecvCopyAwaiter::Finalize(void* owner) noexcept {
  auto* self = static_cast<RecvCopyAwaiter*>(owner);
  ALYRN_CHECK(self->Operation()->TryAuthorizeCoupledRelease(),
              "Uring Recv() released its stream slot before result readiness");
  detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kRead,
                                       self);
}

// --- SendAwaiter ---
bool Stream::SendAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (buffer_.empty()) {
    auto available = detail::StreamOperationSlot::ValidateAvailable(
        *stream_, detail::StreamOperationDirection::kWrite);
    if (!available.has_value()) {
      Operation()->SetImmediateError(available.error());
      return false;
    }
    Operation()->SetImmediateSuccess();
    return false;
  }
  auto reserved = detail::StreamOperationSlot::Reserve(
      *stream_, detail::StreamOperationDirection::kWrite, this);
  if (!reserved.has_value()) {
    Operation()->SetImmediateError(reserved.error());
    return false;
  }

  Operation()->kind = OpKind::kWriteComplete;
  auto begun = stream_->BeginWriteOperation(Operation(), this, &SendAwaiter::Finalize);
  if (!begun.has_value()) {
    stream_->AbortWriteOperation(Operation());
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kWrite, this);
    Operation()->SetImmediateError(begun.error());
    return false;
  }
  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Operation(), continuation,
      detail::PrepareSend(stream_->fd_, buffer_.data(), buffer_.size(), MSG_NOSIGNAL),
      [this](Error error) noexcept {
        stream_->AbortWriteOperation(Operation());
        detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kWrite,
                                             this);
        Operation()->SetImmediateError(error);
      });
}

Result<std::size_t> Stream::SendAwaiter::await_resume() noexcept {
  return ToSizeResult(Operation()->result, timed_out_);
}

void Stream::SendAwaiter::OnComplete(::alyrn::uring::detail::Op* op) noexcept {
  auto* self = OpHook::OwnerFrom(op);
  self->timed_out_ = self->stream_->WriteDeadlineTimedOut();
  self->stream_->CompleteWriteOperation(op);
}

void Stream::SendAwaiter::Finalize(void* owner) noexcept {
  auto* self = static_cast<SendAwaiter*>(owner);
  ALYRN_CHECK(self->Operation()->TryAuthorizeCoupledRelease(),
              "Uring send released its stream slot before result readiness");
  detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kWrite,
                                       self);
}

// ----- CloseAwaiter ------
class [[nodiscard]] Stream::CloseAwaiter : public detail::OpHook<Stream::CloseAwaiter> {
  friend void detail::DispatchStreamCloseComplete(detail::Op* op) noexcept;

public:
  using OpHook = detail::OpHook<CloseAwaiter>;

  explicit CloseAwaiter(Stream& stream) noexcept
      : OpHook(OpKind::kStreamCloseComplete), stream_(&stream) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    stream_->RequireOwnerLoop();
    if (stream_->pending_close_ != nullptr) {
      convergence_.SetError(Errno(EBUSY));
      return false;
    }

    auto close_prepared = stream_->lifecycle_.PrepareClose();
    if (!close_prepared.has_value()) {
      convergence_.SetError(close_prepared.error());
      return false;
    }
    if (!*close_prepared) {
      convergence_.SetSuccess();
      return false;
    }

    if (stream_->pending_read_ == nullptr && stream_->pending_write_ == nullptr) {
      convergence_.SetResult(CloseFd());
      return false;
    }

    stream_->pending_close_ = this;
    // The cancel CQE may arrive before the pending stream operations drain.
    // Keep the work handle empty until TryComplete() observes both conditions.
    convergence_.BeginWaiting(continuation);
    Operation()->kind = OpKind::kStreamCloseComplete;

    auto submitted = detail::LoopAccess::SubmitOp(*stream_->loop_, Operation(),
                                                  detail::PrepareCancelAllByFd(stream_->fd_));
    if (!submitted.has_value()) {
      stream_->pending_close_ = nullptr;
      stream_->lifecycle_.AbortClosePreparation();
      convergence_.SetError(submitted.error());
      return false;
    }

    return true;
  }

  Result<void> await_resume() noexcept {
    ALYRN_CHECK(convergence_.HasResult(), "Uring stream Close resumed before convergence");
    return convergence_.TakeResult();
  }

  void TryComplete(::alyrn::uring::detail::Op* current = nullptr) noexcept {
    if (stream_ == nullptr || !convergence_.TryAuthorizeClose(stream_->pending_read_ != nullptr ||
                                                              stream_->pending_write_ != nullptr)) {
      return;
    }

    Loop* loop = stream_->loop_;
    stream_->pending_close_ = nullptr;
    convergence_.SetResult(CloseFd());
    stream_ = nullptr;
    // The cancel operation is no longer waiting for a CQE, so its embedded
    // work slot can carry the final coroutine resumption.
    Operation()->resume_work.SetHandle(convergence_.Continuation());
    if (current != Operation()) {
      detail::LoopAccess::ScheduleCompletion(*loop, &Operation()->resume_work);
    }
  }

private:
  static void OnCancelComplete(::alyrn::uring::detail::Op* op) noexcept {
    auto* self = OpHook::OwnerFrom(op);
    self->convergence_.MarkCancelRequestTerminal();
    self->TryComplete(op);
  }

  Result<void> CloseFd() noexcept {
    const int fd = std::exchange(stream_->fd_, -1);
    stream_->lifecycle_.MarkClosed();
    if (fd < 0) {
      return Result<void>{};
    }
    if (::close(fd) < 0) {
      return std::unexpected(CurrentErrno());
    }
    return Result<void>{};
  }

  Stream* stream_;
  detail::FdCloseConvergence convergence_;
};

// --- SendZeroCopyAwaiter ---
bool Stream::SendZeroCopyAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (buffer_.empty()) {
    auto available = detail::StreamOperationSlot::ValidateAvailable(
        *stream_, detail::StreamOperationDirection::kWrite);
    if (!available.has_value()) {
      Operation()->SetImmediateError(available.error());
      return false;
    }
    Operation()->SetImmediateSuccess();
    return false;
  }
  auto reserved = detail::StreamOperationSlot::Reserve(
      *stream_, detail::StreamOperationDirection::kWrite, this);
  if (!reserved.has_value()) {
    Operation()->SetImmediateError(reserved.error());
    return false;
  }

  Operation()->kind = OpKind::kSendZeroCopyComplete;
  auto begun = stream_->BeginWriteOperation(Operation(), this, &SendZeroCopyAwaiter::Finalize);
  if (!begun.has_value()) {
    stream_->AbortWriteOperation(Operation());
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kWrite, this);
    Operation()->SetImmediateError(begun.error());
    return false;
  }
  Operation()->resume_work.SetHandle(continuation);

  auto submitted =
      detail::LoopAccess::SubmitOp(*stream_->loop_, Operation(),
                                   detail::PrepareSendZeroCopyReportUsage(
                                       stream_->fd_, buffer_.data(), buffer_.size(), MSG_NOSIGNAL));
  if (!submitted.has_value()) {
    stream_->AbortWriteOperation(Operation());
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kWrite, this);
    Operation()->SetImmediateError(submitted.error());
    return false;
  }
  return true;
}

Result<ZeroCopySendResult> Stream::SendZeroCopyAwaiter::await_resume() noexcept {
  if (!Operation()->result.HasValue()) {
    return std::unexpected(Errno(EIO));
  }
  const int result = *Operation()->result;
  if (timed_out_) {
    return std::unexpected(Errno(ETIMEDOUT));
  }
  if (result < 0) {
    return std::unexpected(NegErrno(result));
  }
  return ZeroCopySendResult{
      .bytes = static_cast<std::size_t>(result),
      .usage = usage_,
      .notification_received = notification_received_,
  };
}

CompletionDisposition Stream::SendZeroCopyAwaiter::OnComplete(::alyrn::uring::detail::Op* op,
                                                              CompletionEvent event) noexcept {
  auto* self = OpHook::OwnerFrom(op);
  CompletionDisposition disposition;

  if (event.Notification()) {
    const bool physical_terminal = self->lifecycle_.MarkPhysicalTerminal();
    if (!physical_terminal) {
      return disposition;
    }

    self->notification_received_ = true;
    // A send-zc notification stores usage bits in cqe_res, so it is not an
    // errno result even when its high bit is set.
    self->usage_ =
        event.ZeroCopyWasCopied() ? ZeroCopySendUsage::kCopied : ZeroCopySendUsage::kZeroCopy;
    disposition.kernel_request_terminal = true;
    disposition.decrement_inflight = true;
  } else if (self->lifecycle_.RecordLogicalResult()) {
    op->result = event.result;
    self->timed_out_ = self->stream_->WriteDeadlineTimedOut();
    // F_MORE is the kernel's indication that a notification CQE will follow.
    // Without it, the primary CQE is already the physical terminal boundary,
    // including primary error paths that never borrowed the caller buffer.
    if (!event.More() && self->lifecycle_.MarkPhysicalTerminal()) {
      disposition.kernel_request_terminal = true;
      disposition.decrement_inflight = true;
    }
    // Keep a primary -errno as a raw kernel result. Write() may recover
    // ENOMEM only after this awaiter has crossed its release boundary.
  }

  if (self->lifecycle_.PhysicalTerminal()) {
    self->stream_->CompleteWriteOperation(op);
  }
  disposition.resume_continuation = self->lifecycle_.ContinuationAuthorized();
  return disposition;
}

void Stream::SendZeroCopyAwaiter::Finalize(void* owner) noexcept {
  auto* self = static_cast<SendZeroCopyAwaiter*>(owner);
  ALYRN_CHECK(self->lifecycle_.TryAuthorizeRelease(),
              "Uring send-zc released its stream slot before physical terminal");
  detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kWrite,
                                       self);
  ALYRN_CHECK(self->lifecycle_.TryAuthorizeContinuation(),
              "Uring send-zc continuation was not authorized after release");
}

namespace detail {

void DispatchStreamReadComplete(::alyrn::uring::detail::Op* op) noexcept {
  Stream::ReadAwaiter::OnComplete(op);
}

void DispatchStreamRecvComplete(::alyrn::uring::detail::Op* op) noexcept {
  Stream::RecvAwaiter::OnComplete(op);
}

void DispatchStreamRecvCopyComplete(::alyrn::uring::detail::Op* op,
                                    CompletionEvent event) noexcept {
  Stream::RecvCopyAwaiter::OnComplete(op, event);
}

void DispatchStreamWriteComplete(::alyrn::uring::detail::Op* op) noexcept {
  Stream::SendAwaiter::OnComplete(op);
}

CompletionDisposition DispatchSendZeroCopyComplete(::alyrn::uring::detail::Op* op,
                                                   CompletionEvent event) noexcept {
  return Stream::SendZeroCopyAwaiter::OnComplete(op, event);
}

void DispatchStreamCloseComplete(::alyrn::uring::detail::Op* op) noexcept {
  Stream::CloseAwaiter::OnCancelComplete(op);
}

CompletionDisposition DispatchStreamReadCancelComplete(::alyrn::uring::detail::Op* op) noexcept {
  return Stream::DeadlineCancelOp::OnComplete(op);
}

CompletionDisposition DispatchStreamWriteCancelComplete(::alyrn::uring::detail::Op* op) noexcept {
  return Stream::DeadlineCancelOp::OnComplete(op);
}

}  // namespace detail

Stream::Stream(Loop* loop, int fd, net::Endpoint peer) noexcept
    : loop_(loop),
      fd_(fd),
      peer_(peer),
      read_cancel_op_(*this, true),
      write_cancel_op_(*this, false) {
  ALYRN_CHECK(loop_ != nullptr, "Stream requires an owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Stream created from wrong Loop thread");
  ALYRN_CHECK(fd_ >= 0, "Stream requires a valid file descriptor");
}

Stream::Stream(Stream&& other) noexcept
    : loop_(PrepareMove(other)),
      fd_(std::exchange(other.fd_, -1)),
      peer_(other.peer_),
      zero_copy_writes_enabled_(other.zero_copy_writes_enabled_),
      lifecycle_(std::move(other.lifecycle_)),
      read_deadline_(std::move(other.read_deadline_)),
      write_deadline_(std::move(other.write_deadline_)),
      read_cancel_op_(*this, true),
      write_cancel_op_(*this, false) {}

Stream& Stream::operator=(Stream&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  Loop* other_loop = PrepareMove(other);
  ALYRN_CHECK(loop_ == nullptr || loop_ == other_loop,
              "Stream move requires both objects to use the same Loop");
  if (loop_ != nullptr) {
    ResetForMove();
  }

  loop_ = other_loop;
  fd_ = std::exchange(other.fd_, -1);
  peer_ = other.peer_;
  pending_read_ = nullptr;
  pending_write_ = nullptr;
  pending_close_ = nullptr;
  zero_copy_writes_enabled_ = other.zero_copy_writes_enabled_;
  lifecycle_ = std::move(other.lifecycle_);
  read_deadline_ = std::move(other.read_deadline_);
  write_deadline_ = std::move(other.write_deadline_);
  return *this;
}

Stream::~Stream() noexcept {
  // Idle drop closes the descriptor. Pending SQEs still hold user_data in the
  // coroutine frame, so destroying this object while a read, write, or close
  // is in flight remains a contract violation until operations live off-frame.
  ALYRN_CHECK(pending_read_ == nullptr, "Stream destroyed with a pending read");
  ALYRN_CHECK(pending_write_ == nullptr, "Stream destroyed with a pending write");
  ALYRN_CHECK(pending_close_ == nullptr, "Stream destroyed with a pending close");
  ALYRN_CHECK(read_deadline_target_ == nullptr,
              "Stream destroyed with a pending read deadline cancellation");
  ALYRN_CHECK(write_deadline_target_ == nullptr,
              "Stream destroyed with a pending write deadline cancellation");
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

void Stream::RequireOwnerLoop() const noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Stream operation has no owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Stream operation called from wrong Loop thread");
}

Stream::ReadAwaiter Stream::Read(std::span<std::byte> buffer) noexcept {
  return ReadAwaiter{*this, buffer};
}

Stream::RecvCopyAwaiter Stream::Recv() noexcept { return RecvCopyAwaiter{*this}; }

Stream::RecvAwaiter Stream::Recv(net::Buffer buffer, std::size_t reserve) noexcept {
  return RecvAwaiter{*this, std::move(buffer), reserve};
}

Stream::SendAwaiter Stream::Send(std::span<const std::byte> buffer) noexcept {
  return SendAwaiter{*this, buffer};
}

coro::Task<Result<void>> Stream::Write(std::span<const std::byte> buffer) {
  if (buffer.empty()) {
    auto available = detail::StreamOperationSlot::ValidateAvailable(
        *this, detail::StreamOperationDirection::kWrite);
    if (!available.has_value()) {
      co_return std::unexpected(available.error());
    }
    co_return Result<void>{};
  }

  while (!buffer.empty()) {
    if (ZeroCopyWritesEnabled()) {
      auto sent = co_await SendZeroCopy(buffer);
      if (sent.has_value()) {
        if (sent->bytes == 0) {
          co_return std::unexpected(Errno(EPIPE));
        }
        buffer = buffer.subspan(sent->bytes);
        continue;
      }

      // SendZeroCopy() has crossed its kernel release boundary before
      // returning, so an ENOMEM fallback cannot race the kernel's access to
      // buffer.
      if (sent.error().value() != ENOMEM) {
        co_return std::unexpected(sent.error());
      }
    }

    auto written = co_await Send(buffer);
    if (!written.has_value()) {
      co_return std::unexpected(written.error());
    }
    if (*written == 0) {
      co_return std::unexpected(Errno(EPIPE));
    }
    buffer = buffer.subspan(*written);
  }

  co_return Result<void>{};
}

coro::Task<Result<void>> Stream::Shutdown() noexcept { return CloseWrite(); }

coro::Task<Result<void>> Stream::CloseWrite() noexcept {
  RequireOwnerLoop();
  if (fd_ < 0) {
    co_return std::unexpected(Errno(EBADF));
  }
  auto shutdown = lifecycle_.PrepareShutdown(pending_write_ != nullptr);
  if (!shutdown.has_value()) {
    co_return std::unexpected(shutdown.error());
  }
  if (!*shutdown) {
    co_return Result<void>{};
  }
  if (::shutdown(fd_, SHUT_WR) < 0) {
    lifecycle_.AbortShutdownPreparation();
    co_return std::unexpected(CurrentErrno());
  }
  lifecycle_.CommitShutdown();

  co_return Result<void>{};
}

coro::Task<Result<void>> Stream::CloseRead() noexcept {
  RequireOwnerLoop();
  if (fd_ < 0) {
    co_return std::unexpected(Errno(EBADF));
  }
  auto prepare = lifecycle_.PrepareCloseRead(pending_read_ != nullptr);
  if (!prepare.has_value()) {
    co_return std::unexpected(prepare.error());
  }
  if (!*prepare) {
    co_return Result<void>{};
  }
  if (::shutdown(fd_, SHUT_RD) < 0) {
    lifecycle_.AbortCloseReadPreparation();
    co_return std::unexpected(CurrentErrno());
  }
  lifecycle_.CommitCloseRead();
  co_return Result<void>{};
}

coro::Task<Result<void>> Stream::Close() noexcept { co_return co_await CloseAwaiter(*this); }

Result<net::Endpoint> Stream::LocalAddr() const noexcept {
  RequireOwnerLoop();

  if (fd_ < 0) {
    return std::unexpected(Errno(EBADF));
  }

  return net::GetLocalEndpoint(fd_);
}

Result<void> Stream::SetNoDelay(bool enabled) const noexcept {
  RequireOwnerLoop();
  return net::SetNoDelay(fd_, enabled);
}

Result<void> Stream::SetKeepAlive(bool enabled) const noexcept {
  RequireOwnerLoop();
  return net::SetKeepAlive(fd_, enabled);
}

Result<void> Stream::SetKeepAlivePeriod(time::Duration period) const noexcept {
  RequireOwnerLoop();
  return net::SetKeepAlivePeriod(fd_, period);
}

Result<void> Stream::SetReadBuffer(std::size_t bytes) const noexcept {
  RequireOwnerLoop();
  return net::SetReadBuffer(fd_, bytes);
}

Result<void> Stream::SetWriteBuffer(std::size_t bytes) const noexcept {
  RequireOwnerLoop();
  return net::SetWriteBuffer(fd_, bytes);
}

Result<void> Stream::SetDeadline(std::optional<time::Deadline> deadline) noexcept {
  auto read_result = SetReadDeadline(deadline);
  if (!read_result.has_value()) {
    return read_result;
  }
  return SetWriteDeadline(deadline);
}

Result<void> Stream::SetReadDeadline(std::optional<time::Deadline> deadline) noexcept {
  RequireOwnerLoop();
  CancelReadDeadlineTimer();
  read_deadline_ = deadline;
  if (read_deadline_target_ != nullptr && !read_cancel_requested_ && !ArmReadDeadlineTimer()) {
    return std::unexpected(Errno(ENOMEM));
  }
  return {};
}

Result<void> Stream::SetWriteDeadline(std::optional<time::Deadline> deadline) noexcept {
  RequireOwnerLoop();
  CancelWriteDeadlineTimer();
  write_deadline_ = deadline;
  if (write_deadline_target_ != nullptr && !write_cancel_requested_ && !ArmWriteDeadlineTimer()) {
    return std::unexpected(Errno(ENOMEM));
  }
  return {};
}

Stream::SendZeroCopyAwaiter Stream::SendZeroCopy(std::span<const std::byte> buffer) noexcept {
  return SendZeroCopyAwaiter{*this, buffer};
}

void Stream::NotifyCloseProgress() noexcept {
  if (pending_close_ != nullptr) {
    pending_close_->TryComplete();
  }
}

void Stream::ResetForMove() noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Stream move destination is not initialized");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Stream move called from wrong Loop thread");
  ALYRN_CHECK(pending_read_ == nullptr, "Stream move destination has a pending read");
  ALYRN_CHECK(pending_write_ == nullptr, "Stream move destination has a pending write");
  ALYRN_CHECK(pending_close_ == nullptr, "Stream move destination has a pending close");

  const int fd = std::exchange(fd_, -1);
  if (fd >= 0) {
    ::close(fd);
  }
}

Loop* Stream::PrepareMove(Stream& other) noexcept {
  ALYRN_CHECK(other.loop_ != nullptr, "Stream move source is not initialized");
  ALYRN_CHECK(other.loop_->IsInLoopThread(), "Stream move called from wrong Loop thread");
  ALYRN_CHECK(other.pending_read_ == nullptr, "Stream cannot move with a pending read operation");
  ALYRN_CHECK(other.pending_write_ == nullptr, "Stream cannot move with a pending write operation");
  ALYRN_CHECK(other.pending_close_ == nullptr, "Stream cannot move with a pending close operation");

  Loop* loop = std::exchange(other.loop_, nullptr);
  return loop;
}

}  // namespace alyrn::uring
