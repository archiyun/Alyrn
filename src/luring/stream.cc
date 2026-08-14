// SPDX-License-Identifier: MIT
#include "coropact/luring/stream.h"

#include <liburing.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <new>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/result.h"
#include "coropact/luring/detail/fd_close_convergence.h"
#include "coropact/luring/detail/loop_access.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/detail/operation_submission.h"
#include "coropact/luring/detail/sqe_prep.h"
#include "coropact/luring/detail/stream_operation_slot.h"
#include "coropact/luring/loop.h"
#include "coropact/net/endpoint.h"

namespace coropact::luring {

using namespace detail;

namespace {

constexpr std::size_t kReadIntoMaxIov = 16;

Result<std::size_t> ToSizeResult(const LUringCqeResult& result) noexcept {
  COROPACT_CHECK(result.HasValue(),
                 "LUring stream awaiter resumed before its CQE result was ready");
  const int cqe_result = *result;
  if (cqe_result < 0) {
    return std::unexpected(NegErrno(cqe_result));
  }
  return static_cast<std::size_t>(cqe_result);
}

}  // namespace

Result<void> detail::StreamOperationSlot::Validate(
    LUringStream& stream, StreamOperationDirection direction) noexcept {
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

Result<void> detail::StreamOperationSlot::Reserve(LUringStream& stream,
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
    LUringStream& stream, StreamOperationDirection direction) noexcept {
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

void detail::StreamOperationSlot::Release(LUringStream& stream, StreamOperationDirection direction,
                                          void* operation) noexcept {
  void*& pending =
      direction == StreamOperationDirection::kRead ? stream.pending_read_ : stream.pending_write_;
  if (pending == operation) {
    pending = nullptr;
    stream.NotifyCloseProgress();
  }
}

// ---- ReadSomeAwaiter ---
bool LUringStream::ReadSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  auto reserved =
      detail::StreamOperationSlot::Reserve(*stream_, detail::StreamOperationDirection::kRead, this);
  if (!reserved.has_value()) {
    Op()->SetImmediateError(reserved.error());
    return false;
  }
  if (buffer_.empty()) {
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
    Op()->SetImmediateSuccess();
    return false;
  }

  Op()->kind = LUringOpKind::kReadComplete;
  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Op(), continuation,
      detail::PrepareRecv(stream_->fd_, buffer_.data(), buffer_.size()),
      [this](Error error) noexcept {
        detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead,
                                             this);
        Op()->SetImmediateError(error);
      });
}

Result<std::size_t> LUringStream::ReadSomeAwaiter::await_resume() noexcept {
  return ToSizeResult(Op()->result);
}

void LUringStream::ReadSomeAwaiter::OnComplete(LUringOp* op) noexcept {
  auto* self = OpHook::OwnerFrom(op);
  COROPACT_CHECK(op->TryAuthorizeCoupledRelease(),
                 "LUring ReadSome released its stream slot before result readiness");
  if (self->stream_ != nullptr) {
    detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kRead,
                                         self);
  }
}

// ---- ReadIntoAwaiter ---
bool LUringStream::ReadIntoAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  auto reserved =
      detail::StreamOperationSlot::Reserve(*stream_, detail::StreamOperationDirection::kRead, this);
  if (!reserved.has_value()) {
    Op()->SetImmediateError(reserved.error());
    return false;
  }
  iovec single_iov{};
  const ReservationKind reservation = PrepareReservation(single_iov);
  if (reservation == ReservationKind::kNone) {
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
    Op()->SetImmediateError(Errno(ENOMEM));
    return false;
  }

  Op()->kind = LUringOpKind::kReadIntoComplete;

  auto on_submit_failure = [this](Error error) noexcept {
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
    FinishReservation(std::unexpected(error));
    Op()->SetImmediateError(error);
  };

  if (reservation == ReservationKind::kSingle) {
    return detail::SubmitAwaitingOperation(
        *stream_->loop_, *Op(), continuation,
        detail::PrepareRecv(stream_->fd_, single_iov.iov_base, single_iov.iov_len),
        std::move(on_submit_failure));
  }

  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Op(), continuation,
      detail::PrepareReadv(stream_->fd_, iovs_.data(), static_cast<unsigned>(iovs_.size()), -1),
      std::move(on_submit_failure));
}

net::ReadIntoOutcome LUringStream::ReadIntoAwaiter::await_resume() noexcept {
  return {
      .result = ToSizeResult(Op()->result),
      .buffer = std::move(buffer_),
  };
}

void LUringStream::ReadIntoAwaiter::OnComplete(LUringOp* op) noexcept {
  auto* self = OpHook::OwnerFrom(op);
  COROPACT_CHECK(op->TryAuthorizeCoupledRelease(),
                 "LUring ReadInto released its reservation before result readiness");
  self->FinishReservation(ToSizeResult(op->result));
  if (self->stream_ != nullptr) {
    detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kRead,
                                         self);
  }
}

LUringStream::ReadIntoAwaiter::ReservationKind LUringStream::ReadIntoAwaiter::PrepareReservation(
    iovec& single_iov) noexcept {
  try {
    if (auto iov = buffer_.TryPrepareWriteOne(reserve_); iov.has_value()) {
      single_iov = *iov;
      reservation_kind_ = ReservationKind::kSingle;
    } else {
      auto iovs = buffer_.PrepareWrite(reserve_, kReadIntoMaxIov);
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

void LUringStream::ReadIntoAwaiter::FinishReservation(Result<std::size_t> result) noexcept {
  COROPACT_CHECK(reservation_kind_ != ReservationKind::kNone,
                 "ReadIntoAwaiter completion without a buffer reservation");
  if (result.has_value()) {
    buffer_.CommitWrite(*result);
  } else {
    buffer_.AbortWrite();
  }
  iovs_.clear();
  reservation_kind_ = ReservationKind::kNone;
}

// --- ReadSomeForAwaiter ---
LUringStream::ReadSomeForAwaiter::ReadSomeForAwaiter(LUringStream& stream,
                                                     std::span<std::byte> buffer,
                                                     time::Duration timeout) noexcept
    : ReadOpHook(LUringOpKind::kTimedReadComplete),
      TimeoutOpHook(LUringOpKind::kTimedReadTimeoutComplete),
      stream_(&stream),
      buffer_(buffer) {
  const auto milliseconds = std::max(
      std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count(), std::int64_t{1});
  timeout_ts_.tv_sec = static_cast<__kernel_time64_t>(milliseconds / 1000);
  timeout_ts_.tv_nsec = static_cast<long>(milliseconds % 1000) * 1'000'000;
}

bool LUringStream::ReadSomeForAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  auto reserved =
      detail::StreamOperationSlot::Reserve(*stream_, detail::StreamOperationDirection::kRead, this);
  if (!reserved.has_value()) {
    ReadOp()->SetImmediateError(reserved.error());
    return false;
  }
  if (buffer_.empty()) {
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
    ReadOp()->SetImmediateSuccess();
    return false;
  }

  continuation_ = continuation;

  auto submitted = detail::LoopAccess::SubmitOp(
      *stream_->loop_, ReadOp(),
      detail::PrepareLinkedRecv(stream_->fd_, buffer_.data(), buffer_.size()));
  if (!submitted.has_value()) {
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
    ReadOp()->SetImmediateError(submitted.error());
    return false;
  }

  submitted = detail::LoopAccess::SubmitOp(
      *stream_->loop_, TimeoutOp(), detail::PrepareLinkTimeout(&timeout_ts_, 0));
  if (!submitted.has_value()) {
    // The receive is already queued. It will complete normally without the
    // optional timeout, and the awaiter remains alive until that CQE.
    (void)(lifecycle_.RecordMemberCompletion(operation::detail::CompositeMember::kSecond));
  }
  return true;
}

Result<std::size_t> LUringStream::ReadSomeForAwaiter::await_resume() noexcept {
  if (!ReadOp()->CqeCompletionRecorded()) {
    return ToSizeResult(ReadOp()->result);
  }

  COROPACT_CHECK(lifecycle_.MemberCompleted(operation::detail::CompositeMember::kFirst),
                 "timed read resumed before its read member settled");
  if (ReadOp()->result.HasValue() && *ReadOp()->result >= 0) {
    return static_cast<std::size_t>(*ReadOp()->result);
  }
  if (TimeoutOp()->result.HasValue() && *TimeoutOp()->result == -ETIME) {
    return std::unexpected(Errno(ETIMEDOUT));
  }
  return ToSizeResult(ReadOp()->result);
}

void LUringStream::ReadSomeForAwaiter::OnReadComplete(LUringOp* op) noexcept {
  ReadOpHook::OwnerFrom(op)->CompleteRead(op);
}

void LUringStream::ReadSomeForAwaiter::OnTimeoutComplete(LUringOp* op) noexcept {
  TimeoutOpHook::OwnerFrom(op)->CompleteTimeout(op);
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

  if (lifecycle_.TryAuthorizeRelease() && stream_ != nullptr) {
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead, this);
  }
  if (lifecycle_.TryAuthorizeContinuation()) {
    current->resume_work.SetHandle(continuation_);
  }
}

// --- SendAwaiter ---
bool LUringStream::SendAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (buffer_.empty()) {
    auto available = detail::StreamOperationSlot::ValidateAvailable(
        *stream_, detail::StreamOperationDirection::kWrite);
    if (!available.has_value()) {
      Op()->SetImmediateError(available.error());
      return false;
    }
    Op()->SetImmediateSuccess();
    return false;
  }
  auto reserved = detail::StreamOperationSlot::Reserve(
      *stream_, detail::StreamOperationDirection::kWrite, this);
  if (!reserved.has_value()) {
    Op()->SetImmediateError(reserved.error());
    return false;
  }

  Op()->kind = LUringOpKind::kWriteComplete;
  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Op(), continuation,
      detail::PrepareSend(stream_->fd_, buffer_.data(), buffer_.size(), MSG_NOSIGNAL),
      [this](Error error) noexcept {
        detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kWrite,
                                             this);
        Op()->SetImmediateError(error);
      });
}

Result<std::size_t> LUringStream::SendAwaiter::await_resume() noexcept {
  return ToSizeResult(Op()->result);
}

void LUringStream::SendAwaiter::OnComplete(LUringOp* op) noexcept {
  auto* self = OpHook::OwnerFrom(op);
  COROPACT_CHECK(op->TryAuthorizeCoupledRelease(),
                 "LUring send released its stream slot before result readiness");
  if (self->stream_ != nullptr) {
    detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kWrite,
                                         self);
  }
}

// ----- CloseAwaiter ------
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
    Op()->kind = LUringOpKind::kStreamCloseComplete;

    auto submitted = detail::LoopAccess::SubmitOp(
        *stream_->loop_, Op(),
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
    COROPACT_CHECK(convergence_.HasResult(), "LUring stream Close resumed before convergence");
    return convergence_.TakeResult();
  }

  void TryComplete(LUringOp* current = nullptr) noexcept {
    if (stream_ == nullptr || !convergence_.TryAuthorizeClose(stream_->pending_read_ != nullptr ||
                                                              stream_->pending_write_ != nullptr)) {
      return;
    }

    LUringLoop* loop = stream_->loop_;
    stream_->pending_close_ = nullptr;
    convergence_.SetResult(CloseFd());
    stream_ = nullptr;
    // The cancel operation is no longer waiting for a CQE, so its embedded
    // work slot can carry the final coroutine resumption.
    Op()->resume_work.SetHandle(convergence_.Continuation());
    if (current != Op()) {
      detail::LoopAccess::ScheduleCompletion(*loop, &Op()->resume_work);
    }
  }

private:
  static void OnCancelComplete(LUringOp* op) noexcept {
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

  LUringStream* stream_;
  detail::FdCloseConvergence convergence_;
};

// --- SendZeroCopyAwaiter ---
bool LUringStream::SendZeroCopyAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  if (buffer_.empty()) {
    auto available = detail::StreamOperationSlot::ValidateAvailable(
        *stream_, detail::StreamOperationDirection::kWrite);
    if (!available.has_value()) {
      Op()->SetImmediateError(available.error());
      return false;
    }
    Op()->SetImmediateSuccess();
    return false;
  }
  auto reserved = detail::StreamOperationSlot::Reserve(
      *stream_, detail::StreamOperationDirection::kWrite, this);
  if (!reserved.has_value()) {
    Op()->SetImmediateError(reserved.error());
    return false;
  }

  Op()->kind = LUringOpKind::kSendZeroCopyComplete;
  Op()->resume_work.SetHandle(continuation);

  auto submitted = detail::LoopAccess::SubmitOp(
      *stream_->loop_, Op(), detail::PrepareSendZeroCopyReportUsage(
                               stream_->fd_, buffer_.data(), buffer_.size(), MSG_NOSIGNAL));
  if (!submitted.has_value()) {
    detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kWrite, this);
    Op()->SetImmediateError(submitted.error());
    return false;
  }
  return true;
}

Result<ZeroCopySendResult> LUringStream::SendZeroCopyAwaiter::await_resume() noexcept {
  if (!Op()->result.HasValue()) {
    return std::unexpected(Errno(EIO));
  }
  const int result = *Op()->result;
  if (result < 0) {
    return std::unexpected(NegErrno(result));
  }
  return ZeroCopySendResult{
      .bytes = static_cast<std::size_t>(result),
      .usage = usage_,
      .notification_received = notification_received_,
  };
}

CompletionDisposition LUringStream::SendZeroCopyAwaiter::OnComplete(
    LUringOp* op, CompletionEvent event) noexcept {
  auto* self = OpHook::OwnerFrom(op);
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
    self->usage_ = (usage & IORING_NOTIF_USAGE_ZC_COPIED) != 0 ? ZeroCopySendUsage::kCopied
                                                               : ZeroCopySendUsage::kZeroCopy;
    disposition.kernel_request_terminal = true;
    disposition.decrement_inflight = true;
  } else if (self->lifecycle_.RecordLogicalResult()) {
    op->result = event.result;
    // F_MORE is the kernel's indication that a notification CQE will follow.
    // Without it, the primary CQE is already the physical terminal boundary,
    // including primary error paths that never borrowed the caller buffer.
    if (!event.More() && self->lifecycle_.MarkPhysicalTerminal()) {
      disposition.kernel_request_terminal = true;
      disposition.decrement_inflight = true;
    }
    // Keep a primary -errno as a raw kernel result. WriteAll() may recover
    // ENOMEM only after this awaiter has crossed its release boundary.
  }

  if (self->lifecycle_.TryAuthorizeRelease()) {
    if (self->stream_ != nullptr) {
      detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kWrite,
                                           self);
    }
  }
  disposition.resume_continuation = self->lifecycle_.TryAuthorizeContinuation();
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
  LUringStream::SendAwaiter::OnComplete(op);
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
  COROPACT_CHECK(loop_->IsInLoopThread(), "LUringStream created from wrong LUringLoop thread");
  COROPACT_CHECK(fd_ >= 0, "LUringStream requires a valid file descriptor");
}

LUringStream::LUringStream(LUringStream&& other) noexcept
    : loop_(PrepareMove(other)),
      fd_(std::exchange(other.fd_, -1)),
      peer_(std::move(other.peer_)),
      zero_copy_writes_enabled_(other.zero_copy_writes_enabled_),
      lifecycle_(std::move(other.lifecycle_)) {}

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
  peer_ = other.peer_;
  pending_read_ = nullptr;
  pending_write_ = nullptr;
  pending_close_ = nullptr;
  zero_copy_writes_enabled_ = other.zero_copy_writes_enabled_;
  lifecycle_ = std::move(other.lifecycle_);
  return *this;
}

LUringStream::~LUringStream() noexcept {
  COROPACT_CHECK(pending_read_ == nullptr, "LUringStream destroyed with a pending read");
  COROPACT_CHECK(pending_write_ == nullptr, "LUringStream destroyed with a pending write");
  COROPACT_CHECK(pending_close_ == nullptr, "LUringStream destroyed with a pending close");
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

LUringStream::ReadSomeForAwaiter LUringStream::ReadSomeFor(std::span<std::byte> buffer,
                                                           time::Duration timeout) noexcept {
  return ReadSomeForAwaiter{*this, buffer, timeout};
}

LUringStream::SendAwaiter LUringStream::Send(std::span<const std::byte> buffer) noexcept {
  return SendAwaiter{*this, buffer};
}

coro::Task<Result<void>> LUringStream::WriteAll(std::span<const std::byte> buffer) {
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

coro::Task<Result<void>> LUringStream::Shutdown() {
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

coro::Task<Result<void>> LUringStream::Close() { co_return co_await CloseAwaiter(*this); }

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
