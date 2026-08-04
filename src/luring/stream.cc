// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/luring/stream.h"

#include <liburing.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <new>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/luring/detail/close_state.h"
#include "coropact/luring/detail/operation_submission.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/op.h"
#include "coropact/net/endpoint.h"

namespace coropact::luring {

namespace {

base::Result<std::size_t> ToSizeResult(const LUringCqeResult& result) noexcept {
  if (!result.HasValue()) {
    return std::unexpected(result.Error());
  }
  if (*result < 0) {
    return std::unexpected(base::MakeNegErrno(*result));
  }
  return static_cast<std::size_t>(*result);
}

}  // namespace

// ---- ReadSomeAwaiter ---
bool LUringStream::ReadSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->closed_ || stream_->fd_ < 0) {
    Op()->SetImmediateError(base::MakeErrno(EBADF));
    return false;
  }
  if (buffer_.empty()) {
    Op()->SetImmediateSuccess();
    return false;
  }
  if (stream_->pending_read_ != nullptr) {
    Op()->SetImmediateError(base::MakeErrno(EBUSY));
    return false;
  }

  stream_->pending_read_ = this;
  Op()->kind = LUringOpKind::kReadComplete;
  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Op(), continuation,
      [fd = stream_->fd_, buffer = buffer_](io_uring_sqe* sqe) noexcept {
        io_uring_prep_recv(sqe, fd, buffer.data(), buffer.size(), 0);
      },
      [this](base::Error error) noexcept {
        stream_->pending_read_ = nullptr;
        Op()->SetImmediateError(error);
      });
}

base::Result<std::size_t> LUringStream::ReadSomeAwaiter::await_resume() noexcept {
  return ToSizeResult(Op()->result);
}

void LUringStream::ReadSomeAwaiter::OnComplete(LUringOp* op) noexcept {
  auto* self = static_cast<OpHook*>(op)->Owner();
  if (self->stream_ != nullptr && self->stream_->pending_read_ == self) {
    self->stream_->pending_read_ = nullptr;
    self->stream_->NotifyCloseProgress();
  }
}

// ---- ReadIntoAwaiter ---
bool LUringStream::ReadIntoAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->closed_ || stream_->fd_ < 0) {
    Op()->SetImmediateError(base::MakeErrno(EBADF));
    return false;
  }
  if (stream_->pending_read_ != nullptr) {
    Op()->SetImmediateError(base::MakeErrno(EBUSY));
    return false;
  }
  if (!PrepareReservation()) {
    Op()->SetImmediateError(base::MakeErrno(ENOMEM));
    return false;
  }

  stream_->pending_read_ = this;
  Op()->kind = LUringOpKind::kReadIntoComplete;
  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Op(), continuation,
      [fd = stream_->fd_, buffer = writable_](io_uring_sqe* sqe) noexcept {
        io_uring_prep_recv(sqe, fd, buffer.data(), buffer.size(), 0);
      },
      [this](base::Error error) noexcept {
        stream_->pending_read_ = nullptr;
        FinishReservation(std::unexpected(error));
        Op()->SetImmediateError(error);
      });
}

net::ReadIntoOutcome LUringStream::ReadIntoAwaiter::await_resume() noexcept {
  return {
      .result = ToSizeResult(Op()->result),
      .buffer = std::move(buffer_),
  };
}

void LUringStream::ReadIntoAwaiter::OnComplete(LUringOp* op) noexcept {
  auto* self = static_cast<OpHook*>(op)->Owner();
  self->FinishReservation(ToSizeResult(op->result));
  if (self->stream_ != nullptr && self->stream_->pending_read_ == self) {
    self->stream_->pending_read_ = nullptr;
    self->stream_->NotifyCloseProgress();
  }
}

bool LUringStream::ReadIntoAwaiter::PrepareReservation() noexcept {
  try {
    auto iovs = buffer_.PrepareWrite(reserve_, 1);
    if (iovs.empty()) {
      buffer_.AbortWrite();
      return false;
    }
    writable_ = {
        static_cast<std::byte*>(iovs.front().iov_base),
        iovs.front().iov_len,
    };
  } catch (const std::bad_alloc&) {
    buffer_.AbortWrite();
    return false;
  }
  reservation_active_ = true;
  return true;
}

void LUringStream::ReadIntoAwaiter::FinishReservation(base::Result<std::size_t> result) noexcept {
  COROPACT_CHECK(reservation_active_, "ReadIntoAwaiter completion without a buffer reservation");
  if (result.has_value()) {
    buffer_.CommitWrite(*result);
  } else {
    buffer_.AbortWrite();
  }
  writable_ = {};
  reservation_active_ = false;
}

// --- ReadSomeForAwaiter ---
LUringStream::ReadSomeForAwaiter::ReadSomeForAwaiter(LUringStream& stream,
                                                     std::span<std::byte> buffer,
                                                     std::chrono::milliseconds timeout) noexcept
    : ReadOpHook(LUringOpKind::kTimedReadComplete),
      TimeoutOpHook(LUringOpKind::kTimedReadTimeoutComplete),
      stream_(&stream),
      buffer_(buffer) {
  const std::int64_t milliseconds = timeout.count() > 0 ? timeout.count() : 1;
  timeout_ts_.tv_sec = static_cast<__kernel_time64_t>(milliseconds / 1000);
  timeout_ts_.tv_nsec = static_cast<long>(milliseconds % 1000) * 1'000'000;
}

bool LUringStream::ReadSomeForAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->closed_ || stream_->fd_ < 0) {
    ReadOp()->SetImmediateError(base::MakeErrno(EBADF));
    return false;
  }
  if (buffer_.empty()) {
    ReadOp()->SetImmediateSuccess();
    return false;
  }
  if (stream_->pending_read_ != nullptr) {
    ReadOp()->SetImmediateError(base::MakeErrno(EBUSY));
    return false;
  }

  continuation_ = continuation;
  stream_->pending_read_ = this;

  auto submitted = stream_->loop_->SubmitOp(
      ReadOp(), [fd = stream_->fd_, buffer = buffer_](io_uring_sqe* sqe) noexcept {
        io_uring_prep_recv(sqe, fd, buffer.data(), buffer.size(), 0);
        sqe->flags |= IOSQE_IO_LINK;
      });
  if (!submitted.has_value()) {
    stream_->pending_read_ = nullptr;
    ReadOp()->SetImmediateError(submitted.error());
    return false;
  }

  submitted = stream_->loop_->SubmitOp(TimeoutOp(), [this](io_uring_sqe* sqe) noexcept {
    io_uring_prep_link_timeout(sqe, &timeout_ts_, 0);
  });
  if (!submitted.has_value()) {
    // The receive is already queued. It will complete normally without the
    // optional timeout, and the awaiter remains alive until that CQE.
    COROPACT_IGNORE_RESULT(
        lifecycle_.RecordMemberCompletion(operation::detail::CompositeMember::kSecond));
  }
  return true;
}

base::Result<std::size_t> LUringStream::ReadSomeForAwaiter::await_resume() noexcept {
  if (!ReadOp()->IsCompleted()) {
    return ToSizeResult(ReadOp()->result);
  }

  assert(lifecycle_.MemberCompleted(operation::detail::CompositeMember::kFirst));
  if (ReadOp()->result.HasValue() && *ReadOp()->result >= 0) {
    return static_cast<std::size_t>(*ReadOp()->result);
  }
  if (TimeoutOp()->result.HasValue() && *TimeoutOp()->result == -ETIME) {
    return std::unexpected(base::MakeErrno(ETIMEDOUT));
  }
  return ToSizeResult(ReadOp()->result);
}

void LUringStream::ReadSomeForAwaiter::OnReadComplete(LUringOp* op) noexcept {
  static_cast<ReadOpHook*>(op)->Owner()->CompleteRead(op);
}

void LUringStream::ReadSomeForAwaiter::OnTimeoutComplete(LUringOp* op) noexcept {
  static_cast<TimeoutOpHook*>(op)->Owner()->CompleteTimeout(op);
}

void LUringStream::ReadSomeForAwaiter::CompleteRead(LUringOp* current) noexcept {
  if (!lifecycle_.RecordMemberCompletion(operation::detail::CompositeMember::kFirst)) {
    return;
  }
  FinishIfReady(current);
}

void LUringStream::ReadSomeForAwaiter::CompleteTimeout(LUringOp* current) noexcept {
  if (!lifecycle_.RecordMemberCompletion(operation::detail::CompositeMember::kSecond)) {
    return;
  }
  FinishIfReady(current);
}

void LUringStream::ReadSomeForAwaiter::FinishIfReady(LUringOp* current) noexcept {
  if (!lifecycle_.TryAuthorizeLogicalResult()) {
    return;
  }

  if (stream_->pending_read_ == this) {
    stream_->pending_read_ = nullptr;
    stream_->NotifyCloseProgress();
  }
  if (lifecycle_.TryAuthorizeContinuation()) {
    current->resume_work.SetHandle(continuation_);
  }
}

// --- WriteSomeAwaiter ---
bool LUringStream::WriteSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->closed_ || stream_->fd_ < 0) {
    Op()->SetImmediateError(base::MakeErrno(EBADF));
    return false;
  }
  if (buffer_.empty()) {
    Op()->SetImmediateSuccess();
    return false;
  }
  if (stream_->pending_write_ != nullptr) {
    Op()->SetImmediateError(base::MakeErrno(EBUSY));
    return false;
  }

  stream_->pending_write_ = this;
  Op()->kind = LUringOpKind::kWriteComplete;
  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Op(), continuation,
      [fd = stream_->fd_, buffer = buffer_](io_uring_sqe* sqe) noexcept {
        io_uring_prep_send(sqe, fd, buffer.data(), buffer.size(), MSG_NOSIGNAL);
      },
      [this](base::Error error) noexcept {
        stream_->pending_write_ = nullptr;
        Op()->SetImmediateError(error);
      });
}

base::Result<std::size_t> LUringStream::WriteSomeAwaiter::await_resume() noexcept {
  return ToSizeResult(Op()->result);
}

void LUringStream::WriteSomeAwaiter::OnComplete(LUringOp* op) noexcept {
  auto* self = static_cast<OpHook*>(op)->Owner();
  if (self->stream_ != nullptr && self->stream_->pending_write_ == self) {
    self->stream_->pending_write_ = nullptr;
    self->stream_->NotifyCloseProgress();
  }
}

class LUringStream::CloseAwaiter : public detail::LUringOpHook<LUringStream::CloseAwaiter> {
  friend void detail::DispatchStreamCloseComplete(LUringOp* op) noexcept;

public:
  using OpHook = detail::LUringOpHook<CloseAwaiter>;

  explicit CloseAwaiter(LUringStream& stream) noexcept
      : OpHook(LUringOpKind::kStreamCloseComplete), stream_(&stream) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    stream_->RequireOwnerLoop();
    if (stream_->pending_close_ != nullptr) {
      state_.SetError(base::MakeErrno(EBUSY));
      return false;
    }
    if (stream_->closed_ || stream_->fd_ < 0) {
      state_.SetSuccess();
      return false;
    }

    stream_->closed_ = true;
    if (stream_->pending_read_ == nullptr && stream_->pending_write_ == nullptr) {
      state_.SetResult(CloseFd());
      return false;
    }

    stream_->pending_close_ = this;
    // The cancel CQE may arrive before the pending stream operations drain.
    // Keep the work handle empty until TryComplete() observes both conditions.
    continuation_ = continuation;
    CancelOp()->kind = LUringOpKind::kStreamCloseComplete;

    auto submitted =
        stream_->loop_->SubmitOp(CancelOp(), [fd = stream_->fd_](io_uring_sqe* sqe) noexcept {
          io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
        });
    if (!submitted.has_value()) {
      stream_->pending_close_ = nullptr;
      stream_->closed_ = false;
      state_.SetError(submitted.error());
      return false;
    }

    return true;
  }

  base::Result<void> await_resume() noexcept {
    assert(state_.HasResult());
    return state_.TakeResult();
  }

  void TryComplete(LUringOp* current = nullptr) noexcept {
    if (state_.Completed() || stream_ == nullptr || !state_.CancelCompleted()) {
      return;
    }
    if (stream_->pending_read_ != nullptr || stream_->pending_write_ != nullptr) {
      return;
    }

    state_.MarkCompleted();
    LUringLoop* loop = stream_->loop_;
    stream_->pending_close_ = nullptr;
    state_.SetResult(CloseFd());
    stream_ = nullptr;
    // The cancel operation is no longer waiting for a CQE, so its embedded
    // work slot can carry the final coroutine resumption.
    CancelOp()->resume_work.SetHandle(continuation_);
    if (current != CancelOp()) {
      loop->ScheduleCompletion(&CancelOp()->resume_work);
    }
  }

private:
  static void OnCancelComplete(LUringOp* op) noexcept {
    auto* self = static_cast<OpHook*>(op)->Owner();
    self->state_.MarkCancelCompleted();
    self->TryComplete(op);
  }

  LUringOp* CancelOp() noexcept { return static_cast<OpHook*>(this); }

  base::Result<void> CloseFd() noexcept {
    const int fd = std::exchange(stream_->fd_, -1);
    if (fd < 0) {
      return base::Result<void>{};
    }
    if (::close(fd) < 0) {
      return std::unexpected(base::CurrentErrno());
    }
    return base::Result<void>{};
  }

  LUringStream* stream_;
  std::coroutine_handle<> continuation_{};
  detail::LUringCloseState state_;
};

// --- SendZeroCopyAwaiter ---
bool LUringStream::SendZeroCopyAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (diagnostics_ != nullptr) {
    diagnostics_->attempts.fetch_add(1, std::memory_order_relaxed);
  }
  if (stream_->closed_ || stream_->fd_ < 0) {
    RecordFailure(ZeroCopySendErrorKind::kClosed);
    Op()->SetImmediateError(base::MakeErrno(EBADF));
    return false;
  }
  if (!stream_->loop_->HasCapability(NativeFeature::kSendZeroCopy)) {
    RecordFailure(ZeroCopySendErrorKind::kProfileUnavailable);
    Op()->SetImmediateError(base::MakeErrno(ENOTSUP));
    return false;
  }
  if (buffer_.empty()) {
    Op()->SetImmediateSuccess();
    if (diagnostics_ != nullptr) {
      diagnostics_->RecordLogicalCompletion();
    }
    return false;
  }
  if (stream_->pending_write_ != nullptr) {
    RecordFailure(ZeroCopySendErrorKind::kBusy);
    Op()->SetImmediateError(base::MakeErrno(EBUSY));
    return false;
  }

  stream_->pending_write_ = this;
  Op()->kind = LUringOpKind::kSendZeroCopyComplete;
  Op()->resume_work.SetHandle(continuation);

  auto submitted = stream_->loop_->SubmitOp(
      Op(), [fd = stream_->fd_, buffer = buffer_](io_uring_sqe* sqe) noexcept {
        io_uring_prep_send_zc(sqe, fd, buffer.data(), buffer.size(), MSG_NOSIGNAL,
                              IORING_SEND_ZC_REPORT_USAGE);
      });
  if (!submitted.has_value()) {
    stream_->pending_write_ = nullptr;
    RecordFailure(ZeroCopySendErrorKind::kSubmission);
    Op()->SetImmediateError(submitted.error());
    return false;
  }
  return true;
}

base::Result<ZeroCopySendResult> LUringStream::SendZeroCopyAwaiter::await_resume() noexcept {
  if (!Op()->result.HasValue()) {
    RecordFailure(ZeroCopySendErrorKind::kProtocol);
    return std::unexpected(base::MakeErrno(EIO));
  }
  const int result = *Op()->result;
  if (result < 0) {
    return std::unexpected(base::MakeNegErrno(result));
  }
  return ZeroCopySendResult{
      .bytes = static_cast<std::size_t>(result),
      .copied = copied_,
      .notification_received = notification_received_,
  };
}

CompletionDisposition LUringStream::SendZeroCopyAwaiter::OnComplete(
    LUringOp* op, CompletionEvent event) noexcept {
  auto* self = static_cast<OpHook*>(op)->Owner();
  CompletionDisposition disposition;

  if (event.Notification()) {
    const bool physical_terminal = self->lifecycle_.MarkPhysicalTerminal();
    if (!physical_terminal) {
      return disposition;
    }

    self->notification_received_ = true;
    // A send-zc notification stores usage bits in cqe_res.  In particular,
    // IORING_NOTIF_USAGE_ZC_COPIED occupies the high bit, so cqe_res may look
    // negative when viewed as int; it is not a -errno error value.
    const auto usage = static_cast<std::uint32_t>(event.result);
    self->copied_ = (usage & IORING_NOTIF_USAGE_ZC_COPIED) != 0;
    if (self->diagnostics_ != nullptr) {
      self->diagnostics_->RecordNotification(event.result, self->copied_);
    }
    disposition.kernel_request_terminal = true;
    disposition.decrement_inflight = true;
  } else if (self->lifecycle_.RecordLogicalResult()) {
    // REPORT_USAGE keeps a notification boundary for every submitted send.
    // Some kernels do not advertise it through F_MORE on the primary CQE, so
    // F_MORE and a primary -errno cannot be the ownership/lifetime boundary.
    op->result = event.result;
    if (self->diagnostics_ != nullptr) {
      self->diagnostics_->RecordPrimary(event.result);
    }
    // Keep a primary -errno as a raw kernel result. io::WriteAll may recover
    // ENOMEM only after this awaiter has observed the notification boundary.
  }

  if (self->lifecycle_.TryAuthorizeRelease()) {
    if (self->stream_ != nullptr && self->stream_->pending_write_ == self) {
      self->stream_->pending_write_ = nullptr;
      self->stream_->NotifyCloseProgress();
    }
  }
  disposition.resume_continuation = self->lifecycle_.TryAuthorizeContinuation();
  if (disposition.resume_continuation && self->diagnostics_ != nullptr) {
    self->diagnostics_->RecordLogicalCompletion();
  }
  return disposition;
}

namespace detail {

void DispatchStreamReadComplete(LUringOp* op) noexcept {
  LUringStream::ReadSomeAwaiter::OnComplete(op);
}

void DispatchStreamReadIntoComplete(LUringOp* op) noexcept {
  LUringStream::ReadIntoAwaiter::OnComplete(op);
}

void DispatchTimedReadComplete(LUringOp* op) noexcept {
  LUringStream::ReadSomeForAwaiter::OnReadComplete(op);
}

void DispatchTimedReadTimeoutComplete(LUringOp* op) noexcept {
  LUringStream::ReadSomeForAwaiter::OnTimeoutComplete(op);
}

void DispatchStreamWriteComplete(LUringOp* op) noexcept {
  LUringStream::WriteSomeAwaiter::OnComplete(op);
}

CompletionDisposition DispatchSendZeroCopyComplete(LUringOp* op, CompletionEvent event) noexcept {
  return LUringStream::SendZeroCopyAwaiter::OnComplete(op, event);
}

void DispatchStreamCloseComplete(LUringOp* op) noexcept {
  LUringStream::CloseAwaiter::OnCancelComplete(op);
}

}  // namespace detail

LUringStream::LUringStream(LUringLoop* loop, int fd, net::Endpoint peer) noexcept
    : loop_(loop), fd_(fd), peer_(std::move(peer)) {
  COROPACT_CHECK(loop_ != nullptr, "LUringStream requires an owner loop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "LUringStream created from wrong LUringLoop thread");
  COROPACT_CHECK(fd_ >= 0, "LUringStream requires a valid file descriptor");
}

LUringStream::LUringStream(LUringStream&& other) noexcept
    : loop_(PrepareMove(other)),
      fd_(std::exchange(other.fd_, -1)),
      peer_(std::move(other.peer_)),
      pending_read_(nullptr),
      pending_write_(nullptr),
      pending_close_(nullptr),
      zero_copy_writes_enabled_(other.zero_copy_writes_enabled_),
      zero_copy_diagnostics_(other.zero_copy_diagnostics_),
      closed_(other.closed_) {
  other.closed_ = true;
}

LUringStream& LUringStream::operator=(LUringStream&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  LUringLoop* other_loop = PrepareMove(other);
  COROPACT_CHECK(loop_ == nullptr || loop_ == other_loop,
                 "LUringStream move requires both objects to use the same LUringLoop");
  if (loop_ != nullptr) {
    ResetForMove();
  }

  loop_ = other_loop;
  fd_ = std::exchange(other.fd_, -1);
  peer_ = std::move(other.peer_);
  pending_read_ = nullptr;
  pending_write_ = nullptr;
  pending_close_ = nullptr;
  zero_copy_writes_enabled_ = other.zero_copy_writes_enabled_;
  zero_copy_diagnostics_ = other.zero_copy_diagnostics_;
  closed_ = other.closed_;
  other.closed_ = true;
  return *this;
}

LUringStream::~LUringStream() noexcept {
  assert(pending_read_ == nullptr);
  assert(pending_write_ == nullptr);
  assert(pending_close_ == nullptr);
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

void LUringStream::RequireOwnerLoop() const noexcept {
  COROPACT_CHECK(loop_ != nullptr, "LUringStream operation has no owner loop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "LUringStream operation called from wrong LUringLoop thread");
}

LUringStream::ReadSomeAwaiter LUringStream::ReadSome(std::span<std::byte> buffer) noexcept {
  return ReadSomeAwaiter{*this, buffer};
}

LUringStream::ReadIntoAwaiter LUringStream::ReadInto(net::Buffer buffer,
                                                     std::size_t reserve) noexcept {
  return ReadIntoAwaiter{*this, std::move(buffer), reserve};
}

LUringStream::ReadSomeForAwaiter LUringStream::ReadSomeFor(
    std::span<std::byte> buffer, std::chrono::milliseconds timeout) noexcept {
  return ReadSomeForAwaiter{*this, buffer, timeout};
}

LUringStream::WriteSomeAwaiter LUringStream::WriteSome(std::span<const std::byte> buffer) noexcept {
  return WriteSomeAwaiter{*this, buffer};
}

coro::Task<base::Result<void>> LUringStream::Shutdown() {
  RequireOwnerLoop();
  if (closed_ || fd_ < 0) {
    co_return std::unexpected(base::MakeErrno(EBADF));
  }
  if (::shutdown(fd_, SHUT_WR) < 0) {
    co_return std::unexpected(base::CurrentErrno());
  }

  co_return base::Result<void>{};
}

coro::Task<base::Result<void>> LUringStream::Close() { co_return co_await CloseAwaiter(*this); }

LUringStream::SendZeroCopyAwaiter LUringStream::SendZeroCopy(
    std::span<const std::byte> buffer) noexcept {
  return SendZeroCopyAwaiter{*this, buffer};
}

void LUringStream::NotifyCloseProgress() noexcept {
  if (pending_close_ != nullptr) {
    pending_close_->TryComplete();
  }
}

void LUringStream::ResetForMove() noexcept {
  COROPACT_CHECK(loop_ != nullptr, "LUringStream move destination is not initialized");
  COROPACT_CHECK(loop_->IsInLoopThread(), "LUringStream move called from wrong LUringLoop thread");
  COROPACT_CHECK(pending_read_ == nullptr, "LUringStream move destination has a pending read");
  COROPACT_CHECK(pending_write_ == nullptr, "LUringStream move destination has a pending write");
  COROPACT_CHECK(pending_close_ == nullptr, "LUringStream move destination has a pending close");

  const int fd = std::exchange(fd_, -1);
  if (fd >= 0) {
    ::close(fd);
  }
}

LUringLoop* LUringStream::PrepareMove(LUringStream& other) noexcept {
  COROPACT_CHECK(other.loop_ != nullptr, "LUringStream move source is not initialized");
  COROPACT_CHECK(other.loop_->IsInLoopThread(),
                 "LUringStream move called from wrong LUringLoop thread");
  COROPACT_CHECK(other.pending_read_ == nullptr,
                 "LUringStream cannot move with a pending read operation");
  COROPACT_CHECK(other.pending_write_ == nullptr,
                 "LUringStream cannot move with a pending write operation");
  COROPACT_CHECK(other.pending_close_ == nullptr,
                 "LUringStream cannot move with a pending close operation");

  LUringLoop* loop = std::exchange(other.loop_, nullptr);
  return loop;
}

}  // namespace coropact::luring
