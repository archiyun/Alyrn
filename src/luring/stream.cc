// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/luring/stream.h"

#include <liburing.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <array>
#include <cassert>
#include <cerrno>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/ctrack.h"
#include "coropact/base/error.h"
#include "coropact/luring/detail/close_state.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/op.h"
#include "coropact/net/endpoint.h"

namespace coropact::luring {

namespace {

base::Result<std::size_t> ToSizeResult(const LUringCqeResult& result) noexcept {
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  if (*result < 0) {
    return std::unexpected(base::make_neg_errno(*result));
  }
  return static_cast<std::size_t>(*result);
}

}  // namespace

// ---- ReadSomeAwaiter ---
bool LUringStream::ReadSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  COROPACT_CTRACK_SCOPE("luring.read.prepare");
  if (stream_->closed_ || stream_->fd_ < 0) {
    op()->SetImmediateError(base::make_errno(EBADF));
    return false;
  }
  if (buffer_.empty()) {
    op()->SetImmediateSuccess();
    return false;
  }
  if (stream_->pending_read_ != nullptr) {
    op()->SetImmediateError(base::make_errno(EBUSY));
    return false;
  }

  stream_->pending_read_ = this;
  op()->kind = LUringOpKind::kReadComplete;
  op()->resume_work.SetHandle(continuation);

  auto submitted = stream_->loop_->SubmitOp(
      op(), [fd = stream_->fd_, buffer = buffer_](io_uring_sqe* sqe) noexcept {
        io_uring_prep_recv(sqe, fd, buffer.data(), buffer.size(), 0);
      });

  if (!submitted.has_value()) {
    stream_->pending_read_ = nullptr;
    op()->SetImmediateError(submitted.error());
    return false;
  }

  return true;
}

base::Result<std::size_t> LUringStream::ReadSomeAwaiter::await_resume() noexcept {
  COROPACT_CTRACK_SCOPE("luring.read.resume");
  return ToSizeResult(op()->result);
}

void LUringStream::ReadSomeAwaiter::OnComplete(LUringOp* op) noexcept {
  auto* self = static_cast<OpHook*>(op)->owner();
  if (self->stream_ != nullptr && self->stream_->pending_read_ == self) {
    self->stream_->pending_read_ = nullptr;
    self->stream_->NotifyCloseProgress();
  }
}

// --- ReadSomeForAwaiter ---
LUringStream::ReadSomeForAwaiter::ReadSomeForAwaiter(LUringStream& stream,
                                                     std::span<std::byte> buffer,
                                                     std::chrono::milliseconds timeout) noexcept
    : ReadOpHook(LUringOpKind::kTimedReadComplete),
      TimeoutOpHook(LUringOpKind::kTimedReadTimeoutComplete),
      stream_(&stream), buffer_(buffer) {
  const std::int64_t milliseconds = timeout.count() > 0 ? timeout.count() : 1;
  timeout_ts_.tv_sec = static_cast<__kernel_time64_t>(milliseconds / 1000);
  timeout_ts_.tv_nsec = static_cast<long>(milliseconds % 1000) * 1'000'000;
}

bool LUringStream::ReadSomeForAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  COROPACT_CTRACK_SCOPE("luring.read.prepare");
  if (stream_->closed_ || stream_->fd_ < 0) {
    read_op()->SetImmediateError(base::make_errno(EBADF));
    return false;
  }
  if (buffer_.empty()) {
    read_op()->SetImmediateSuccess();
    return false;
  }
  if (stream_->pending_read_ != nullptr) {
    read_op()->SetImmediateError(base::make_errno(EBUSY));
    return false;
  }

  continuation_ = continuation;
  stream_->pending_read_ = this;

  auto submitted = stream_->loop_->SubmitOp(
      read_op(), [fd = stream_->fd_, buffer = buffer_](io_uring_sqe* sqe) noexcept {
        io_uring_prep_recv(sqe, fd, buffer.data(), buffer.size(), 0);
        sqe->flags |= IOSQE_IO_LINK;
      });
  if (!submitted.has_value()) {
    stream_->pending_read_ = nullptr;
    read_op()->SetImmediateError(submitted.error());
    return false;
  }

  submitted = stream_->loop_->SubmitOp(timeout_op(), [this](io_uring_sqe* sqe) noexcept {
    io_uring_prep_link_timeout(sqe, &timeout_ts_, 0);
  });
  if (!submitted.has_value()) {
    // The receive is already queued. It will complete normally without the
    // optional timeout, and the awaiter remains alive until that CQE.
    timeout_done_ = true;
  }
  return true;
}

base::Result<std::size_t> LUringStream::ReadSomeForAwaiter::await_resume() noexcept {
  COROPACT_CTRACK_SCOPE("luring.read.resume");
  if (!read_op()->IsCompleted()) {
    return ToSizeResult(read_op()->result);
  }

  assert(read_done_);
  if (read_op()->result.has_value() && *read_op()->result >= 0) {
    return static_cast<std::size_t>(*read_op()->result);
  }
  if (timeout_op()->result.has_value() && *timeout_op()->result == -ETIME) {
    return std::unexpected(base::make_errno(ETIMEDOUT));
  }
  return ToSizeResult(read_op()->result);
}

void LUringStream::ReadSomeForAwaiter::OnReadComplete(LUringOp* op) noexcept {
  static_cast<ReadOpHook*>(op)->owner()->CompleteRead(op);
}

void LUringStream::ReadSomeForAwaiter::OnTimeoutComplete(LUringOp* op) noexcept {
  static_cast<TimeoutOpHook*>(op)->owner()->CompleteTimeout(op);
}

void LUringStream::ReadSomeForAwaiter::CompleteRead(LUringOp* current) noexcept {
  read_done_ = true;
  FinishIfReady(current);
}

void LUringStream::ReadSomeForAwaiter::CompleteTimeout(LUringOp* current) noexcept {
  timeout_done_ = true;
  FinishIfReady(current);
}

void LUringStream::ReadSomeForAwaiter::FinishIfReady(LUringOp* current) noexcept {
  if (!read_done_ || !timeout_done_) {
    return;
  }

  if (stream_->pending_read_ == this) {
    stream_->pending_read_ = nullptr;
    stream_->NotifyCloseProgress();
  }
  current->resume_work.SetHandle(continuation_);
}

// --- WriteSomeAwaiter ---
bool LUringStream::WriteSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (stream_->closed_ || stream_->fd_ < 0) {
    op()->SetImmediateError(base::make_errno(EBADF));
    return false;
  }
  if (buffer_.empty()) {
    op()->SetImmediateSuccess();
    return false;
  }
  if (stream_->pending_write_ != nullptr) {
    op()->SetImmediateError(base::make_errno(EBUSY));
    return false;
  }

  stream_->pending_write_ = this;
  op()->kind = LUringOpKind::kWriteComplete;
  op()->resume_work.SetHandle(continuation);

  auto submitted = stream_->loop_->SubmitOp(
      op(), [fd = stream_->fd_, buffer = buffer_](io_uring_sqe* sqe) noexcept {
        io_uring_prep_send(sqe, fd, buffer.data(), buffer.size(), MSG_NOSIGNAL);
      });

  if (!submitted.has_value()) {
    stream_->pending_write_ = nullptr;
    op()->SetImmediateError(submitted.error());
    return false;
  }
  return true;
}

base::Result<std::size_t> LUringStream::WriteSomeAwaiter::await_resume() noexcept {
  COROPACT_CTRACK_SCOPE("luring.write.resume");
  return ToSizeResult(op()->result);
}

void LUringStream::WriteSomeAwaiter::OnComplete(LUringOp* op) noexcept {
  auto* self = static_cast<OpHook*>(op)->owner();
  if (self->stream_ != nullptr && self->stream_->pending_write_ == self) {
    self->stream_->pending_write_ = nullptr;
    self->stream_->NotifyCloseProgress();
  }
}

class LUringStream::CloseAwaiter
    : public detail::LUringOpHook<LUringStream::CloseAwaiter> {
  friend void detail::DispatchStreamCloseComplete(LUringOp* op) noexcept;

public:
  using OpHook = detail::LUringOpHook<CloseAwaiter>;

  explicit CloseAwaiter(LUringStream& stream) noexcept
      : OpHook(LUringOpKind::kStreamCloseComplete), stream_(&stream) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (stream_->pending_close_ != nullptr) {
      state_.SetError(base::make_errno(EBUSY));
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
    cancel_op()->kind = LUringOpKind::kStreamCloseComplete;

    auto submitted =
        stream_->loop_->SubmitOp(cancel_op(), [fd = stream_->fd_](io_uring_sqe* sqe) noexcept {
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
    cancel_op()->resume_work.SetHandle(continuation_);
    if (current != cancel_op()) {
      loop->ScheduleCompletion(&cancel_op()->resume_work);
    }
  }

private:
  static void OnCancelComplete(LUringOp* op) noexcept {
    auto* self = static_cast<OpHook*>(op)->owner();
    self->state_.MarkCancelCompleted();
    self->TryComplete(op);
  }

  LUringOp* cancel_op() noexcept { return static_cast<OpHook*>(this); }

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

// --- WriteSomePartsAwaiter ---
bool LUringStream::WriteSomePartsAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  if (stream_->closed_ || stream_->fd_ < 0) {
    op()->SetImmediateError(base::make_errno(EBADF));
    return false;
  }

  if (stream_->pending_write_ != nullptr) {
    op()->SetImmediateError(base::make_errno(EBUSY));
    return false;
  }

  if (buffers_.size() > kMaxParts) {
    op()->SetImmediateError(base::make_errno(EINVAL));
    return false;
  }

  std::size_t count = 0;
  for (const auto& part : buffers_) {
    if (part.bytes.empty()) {
      continue;
    }

    iovecs_[count].iov_base = const_cast<std::byte*>(part.bytes.data());
    iovecs_[count].iov_len = part.bytes.size();
    ++count;
  }

  if (count == 0) {
    op()->SetImmediateSuccess();
    return false;
  }

  message_ = {};
  message_.msg_iov = iovecs_.data();
  message_.msg_iovlen = count;

  stream_->pending_write_ = this;

  op()->resume_work.SetHandle(continuation);

  auto submitted = stream_->loop_->SubmitOp(
      op(), [fd = stream_->fd_, message = &message_](io_uring_sqe* sqe) noexcept {
        io_uring_prep_sendmsg(sqe, fd, message, MSG_NOSIGNAL);
      });

  if (!submitted.has_value()) {
    stream_->pending_write_ = nullptr;
    op()->SetImmediateError(submitted.error());
    return false;
  }

  return true;
}

base::Result<std::size_t> LUringStream::WriteSomePartsAwaiter::await_resume() noexcept {
  return ToSizeResult(op()->result);
}

void LUringStream::WriteSomePartsAwaiter::OnComplete(LUringOp* op) noexcept {
  auto* self = static_cast<OpHook*>(op)->owner();

  if (self->stream_ != nullptr && self->stream_->pending_write_ == self) {
    self->stream_->pending_write_ = nullptr;
    self->stream_->NotifyCloseProgress();
  }
}

namespace detail {

void DispatchStreamReadComplete(LUringOp* op) noexcept {
  LUringStream::ReadSomeAwaiter::OnComplete(op);
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

void DispatchStreamWritePartsComplete(LUringOp* op) noexcept {
  LUringStream::WriteSomePartsAwaiter::OnComplete(op);
}

void DispatchStreamCloseComplete(LUringOp* op) noexcept {
  LUringStream::CloseAwaiter::OnCancelComplete(op);
}

}  // namespace detail

LUringStream::WriteSomePartsAwaiter LUringStream::WriteSome(
    std::span<const io::WritePart> buffers) noexcept {
  return WriteSomePartsAwaiter{*this, buffers};
}

LUringStream::LUringStream(LUringLoop* loop, int fd, net::Endpoint peer) noexcept
    : loop_(loop), fd_(fd), peer_(std::move(peer)) {
  assert(loop_ != nullptr);
  assert(fd_ >= 0);
}

LUringStream::LUringStream(LUringStream&& other) noexcept
    : loop_(PrepareMove(other)),
      fd_(std::exchange(other.fd_, -1)),
      peer_(std::move(other.peer_)),
      pending_read_(nullptr),
      pending_write_(nullptr),
      pending_close_(nullptr),
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

LUringStream::ReadSomeAwaiter LUringStream::ReadSome(std::span<std::byte> buffer) noexcept {
  return ReadSomeAwaiter{*this, buffer};
}

LUringStream::ReadSomeForAwaiter LUringStream::ReadSomeFor(
    std::span<std::byte> buffer, std::chrono::milliseconds timeout) noexcept {
  return ReadSomeForAwaiter{*this, buffer, timeout};
}

LUringStream::WriteSomeAwaiter LUringStream::WriteSome(std::span<const std::byte> buffer) noexcept {
  return WriteSomeAwaiter{*this, buffer};
}

coro::Task<base::Result<void>> LUringStream::Shutdown() {
  if (closed_ || fd_ < 0) {
    co_return std::unexpected(base::make_errno(EBADF));
  }
  if (::shutdown(fd_, SHUT_WR) < 0) {
    co_return std::unexpected(base::CurrentErrno());
  }

  co_return base::Result<void>{};
}

coro::Task<base::Result<void>> LUringStream::Close() { co_return co_await CloseAwaiter(*this); }

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
