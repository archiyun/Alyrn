// SPDX-License-Identifier: MIT
#include "alyrn/uring/stream.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <cstddef>
#include <expected>
#include <new>
#include <span>
#include <utility>

#include "alyrn/backend/loop.h"
#include "alyrn/detail/check.h"
#include "alyrn/uring/detail/fd_close_convergence.h"
#include "alyrn/uring/detail/loop_access.h"
#include "alyrn/uring/detail/op.h"
#include "alyrn/uring/detail/op_hook.h"
#include "alyrn/uring/detail/operation_submission.h"
#include "alyrn/uring/detail/sqe_prep.h"
#include "alyrn/uring/detail/stream_operation_slot.h"
#include "alyrn/uring/loop.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/read_into.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/result.h"
#include "alyrn/time/clock.h"

namespace alyrn::uring {

using namespace detail;

namespace {

constexpr std::size_t kReadIntoMaxIov = 16;

Result<std::size_t> ToSizeResult(const CqeResult& result) noexcept {
  ALYRN_CHECK(result.HasValue(),
                 "Uring stream awaiter resumed before its CQE result was ready");
  const int cqe_result = *result;
  if (cqe_result < 0) {
    return std::unexpected(NegErrno(cqe_result));
  }
  return static_cast<std::size_t>(cqe_result);
}

}  // namespace

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

// ---- ReadSomeAwaiter ---
bool Stream::ReadSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
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
  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Operation(), continuation,
      detail::PrepareRecv(stream_->fd_, buffer_.data(), buffer_.size()),
      [this](Error error) noexcept {
        detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kRead,
                                             this);
        Operation()->SetImmediateError(error);
      });
}

Result<std::size_t> Stream::ReadSomeAwaiter::await_resume() noexcept {
  return ToSizeResult(Operation()->result);
}

void Stream::ReadSomeAwaiter::OnComplete(::alyrn::uring::detail::Op* op) noexcept {
  auto* self = OpHook::OwnerFrom(op);
  ALYRN_CHECK(op->TryAuthorizeCoupledRelease(),
                 "Uring ReadSome released its stream slot before result readiness");
  if (self->stream_ != nullptr) {
    detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kRead,
                                         self);
  }
}

// ---- ReadIntoAwaiter ---
bool Stream::ReadIntoAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
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

  Operation()->kind = OpKind::kReadIntoComplete;

  auto on_submit_failure = [this](Error error) noexcept {
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

net::ReadIntoOutcome Stream::ReadIntoAwaiter::await_resume() noexcept {
  return {
      .result = ToSizeResult(Operation()->result),
      .buffer = std::move(buffer_),
  };
}

void Stream::ReadIntoAwaiter::OnComplete(::alyrn::uring::detail::Op* op) noexcept {
  auto* self = OpHook::OwnerFrom(op);
  ALYRN_CHECK(op->TryAuthorizeCoupledRelease(),
                 "Uring ReadInto released its reservation before result readiness");
  self->FinishReservation(ToSizeResult(op->result));
  if (self->stream_ != nullptr) {
    detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kRead,
                                         self);
  }
}

Stream::ReadIntoAwaiter::ReservationKind Stream::ReadIntoAwaiter::PrepareReservation(
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

void Stream::ReadIntoAwaiter::FinishReservation(Result<std::size_t> result) noexcept {
  ALYRN_CHECK(reservation_kind_ != ReservationKind::kNone,
                 "ReadIntoAwaiter completion without a buffer reservation");
  if (result.has_value()) {
    buffer_.CommitWrite(*result);
  } else {
    buffer_.AbortWrite();
  }
  iovs_.clear();
  reservation_kind_ = ReservationKind::kNone;
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
  return detail::SubmitAwaitingOperation(
      *stream_->loop_, *Operation(), continuation,
      detail::PrepareSend(stream_->fd_, buffer_.data(), buffer_.size(), MSG_NOSIGNAL),
      [this](Error error) noexcept {
        detail::StreamOperationSlot::Release(*stream_, detail::StreamOperationDirection::kWrite,
                                             this);
        Operation()->SetImmediateError(error);
      });
}

Result<std::size_t> Stream::SendAwaiter::await_resume() noexcept {
  return ToSizeResult(Operation()->result);
}

void Stream::SendAwaiter::OnComplete(::alyrn::uring::detail::Op* op) noexcept {
  auto* self = OpHook::OwnerFrom(op);
  ALYRN_CHECK(op->TryAuthorizeCoupledRelease(),
                 "Uring send released its stream slot before result readiness");
  if (self->stream_ != nullptr) {
    detail::StreamOperationSlot::Release(*self->stream_, detail::StreamOperationDirection::kWrite,
                                         self);
  }
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
  Operation()->resume_work.SetHandle(continuation);

  auto submitted =
      detail::LoopAccess::SubmitOp(*stream_->loop_, Operation(),
                                   detail::PrepareSendZeroCopyReportUsage(
                                       stream_->fd_, buffer_.data(), buffer_.size(), MSG_NOSIGNAL));
  if (!submitted.has_value()) {
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

void DispatchStreamReadComplete(::alyrn::uring::detail::Op* op) noexcept {
  Stream::ReadSomeAwaiter::OnComplete(op);
}

void DispatchStreamReadIntoComplete(::alyrn::uring::detail::Op* op) noexcept {
  Stream::ReadIntoAwaiter::OnComplete(op);
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

}  // namespace detail

Stream::Stream(Loop* loop, int fd, net::Endpoint peer) noexcept
    : loop_(loop), fd_(fd), peer_(peer) {
  ALYRN_CHECK(loop_ != nullptr, "Stream requires an owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Stream created from wrong Loop thread");
  ALYRN_CHECK(fd_ >= 0, "Stream requires a valid file descriptor");
}

Stream::Stream(Stream&& other) noexcept
    : loop_(PrepareMove(other)),
      fd_(std::exchange(other.fd_, -1)),
      peer_(other.peer_),
      zero_copy_writes_enabled_(other.zero_copy_writes_enabled_),
      lifecycle_(std::move(other.lifecycle_)) {}

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
  return *this;
}

Stream::~Stream() noexcept {
  // Idle drop closes the descriptor. Pending SQEs still hold user_data in the
  // coroutine frame, so destroying this object while a read, write, or close
  // is in flight remains a contract violation until operations live off-frame.
  ALYRN_CHECK(pending_read_ == nullptr, "Stream destroyed with a pending read");
  ALYRN_CHECK(pending_write_ == nullptr, "Stream destroyed with a pending write");
  ALYRN_CHECK(pending_close_ == nullptr, "Stream destroyed with a pending close");
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

void Stream::RequireOwnerLoop() const noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Stream operation has no owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Stream operation called from wrong Loop thread");
}

Stream::ReadSomeAwaiter Stream::ReadSome(std::span<std::byte> buffer) noexcept {
  return ReadSomeAwaiter{*this, buffer};
}

Stream::ReadIntoAwaiter Stream::ReadInto(net::Buffer buffer, std::size_t reserve) noexcept {
  return ReadIntoAwaiter{*this, std::move(buffer), reserve};
}

Stream::SendAwaiter Stream::Send(std::span<const std::byte> buffer) noexcept {
  return SendAwaiter{*this, buffer};
}

coro::Task<Result<void>> Stream::WriteAll(std::span<const std::byte> buffer) {
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
  ALYRN_CHECK(other.pending_read_ == nullptr,
                 "Stream cannot move with a pending read operation");
  ALYRN_CHECK(other.pending_write_ == nullptr,
                 "Stream cannot move with a pending write operation");
  ALYRN_CHECK(other.pending_close_ == nullptr,
                 "Stream cannot move with a pending close operation");

  Loop* loop = std::exchange(other.loop_, nullptr);
  return loop;
}

}  // namespace alyrn::uring
