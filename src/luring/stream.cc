#include "vexo/luring/stream.h"

#include <liburing.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <coroutine>
#include <cstddef>
#include <expected>
#include <optional>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/luring/loop.h"
#include "vexo/luring/op.h"
#include "vexo/net/inet_address.h"

namespace vexo::luring {

namespace {

base::Result<std::size_t> ToSizeResult(const base::Result<int>& result) noexcept {
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  if (*result < 0) {
    return std::unexpected(base::make_neg_errno(*result));
  }
  return static_cast<std::size_t>(*result);
}

}  // namespace

class LUringStream::ReadAwaiter {
public:
  ReadAwaiter(LUringStream& stream, std::span<std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (stream_->closed_ || stream_->fd_ < 0) {
      immediate_ = std::unexpected(base::make_errno(EBADF));
      return false;
    }
    if (buffer_.empty()) {
      immediate_ = std::size_t{0};
      return false;
    }
    if (stream_->pending_read_ != nullptr) {
      immediate_ = std::unexpected(base::make_errno(EBUSY));
      return false;
    }

    stream_->pending_read_ = this;
    op_.kind = LUringOpKind::kRead;
    op_.continuation_ = continuation;
    op_.resume_work.handle = continuation;
    op_.owner = this;
    op_.on_complete = &ReadAwaiter::OnComplete;

    auto submitted = stream_->loop_->SubmitOp(
        &op_, [fd = stream_->fd_, buffer = buffer_](io_uring_sqe* sqe) noexcept {
          io_uring_prep_recv(sqe, fd, buffer.data(), buffer.size(), 0);
        });
    if (!submitted.has_value()) {
      stream_->pending_read_ = nullptr;
      immediate_ = std::unexpected(submitted.error());
      return false;
    }
    return true;
  }

  base::Result<std::size_t> await_resume() noexcept {
    if (immediate_.has_value()) {
      return std::move(*immediate_);
    }
    assert(op_.completed);
    return ToSizeResult(op_.result);
  }

private:
  static void OnComplete(LUringOp* op) noexcept {
    auto* self = static_cast<ReadAwaiter*>(op->owner);
    if (self->stream_ != nullptr && self->stream_->pending_read_ == self) {
      self->stream_->pending_read_ = nullptr;
      self->stream_->NotifyCloseProgress();
    }
  }

  LUringStream* stream_;
  std::span<std::byte> buffer_;
  LUringOp op_{.kind = LUringOpKind::kRead};
  std::optional<base::Result<std::size_t>> immediate_;
};

class LUringStream::WriteAwaiter {
public:
  WriteAwaiter(LUringStream& stream, std::span<const std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (stream_->closed_ || stream_->fd_ < 0) {
      immediate_ = std::unexpected(base::make_errno(EBADF));
      return false;
    }
    if (buffer_.empty()) {
      immediate_ = std::size_t{0};
      return false;
    }
    if (stream_->pending_write_ != nullptr) {
      immediate_ = std::unexpected(base::make_errno(EBUSY));
      return false;
    }

    stream_->pending_write_ = this;
    op_.kind = LUringOpKind::kWrite;
    op_.continuation_ = continuation;
    op_.resume_work.handle = continuation;
    op_.owner = this;
    op_.on_complete = &WriteAwaiter::OnComplete;

    auto submitted = stream_->loop_->SubmitOp(
        &op_, [fd = stream_->fd_, buffer = buffer_](io_uring_sqe* sqe) noexcept {
          io_uring_prep_send(sqe, fd, buffer.data(), buffer.size(), MSG_NOSIGNAL);
        });

    if (!submitted.has_value()) {
      stream_->pending_write_ = nullptr;
      immediate_ = std::unexpected(submitted.error());
      return false;
    }
    return true;
  }
  base::Result<std::size_t> await_resume() noexcept {
    if (immediate_.has_value()) {
      return std::move(*immediate_);
    }
    assert(op_.completed);
    return ToSizeResult(op_.result);
  }

private:
  static void OnComplete(LUringOp* op) noexcept {
    auto* self = static_cast<WriteAwaiter*>(op->owner);
    if (self->stream_ != nullptr && self->stream_->pending_write_ == self) {
      self->stream_->pending_write_ = nullptr;
      self->stream_->NotifyCloseProgress();
    }
  }

  LUringStream* stream_;
  std::span<const std::byte> buffer_;
  LUringOp op_{.kind = LUringOpKind::kWrite};
  std::optional<base::Result<std::size_t>> immediate_;
};

class LUringStream::CloseAwaiter {
public:
  explicit CloseAwaiter(LUringStream& stream) noexcept : stream_(&stream) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (stream_->pending_close_ != nullptr) {
      result_ = std::unexpected(base::make_errno(EBUSY));
      return false;
    }
    if (stream_->closed_ || stream_->fd_ < 0) {
      result_ = base::Result<void>{};
      return false;
    }

    stream_->closed_ = true;
    if (stream_->pending_read_ == nullptr && stream_->pending_write_ == nullptr) {
      result_ = CloseFd();
      return false;
    }

    stream_->pending_close_ = this;
    resume_work_.handle = continuation;
    cancel_op_.kind = LUringOpKind::kClose;
    cancel_op_.owner = this;
    cancel_op_.on_complete = &CloseAwaiter::OnCancelComplete;

    auto submitted = stream_->loop_->SubmitOp(&cancel_op_, [fd = stream_->fd_](
                                                               io_uring_sqe* sqe) noexcept {
      io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
    });
    if (!submitted.has_value()) {
      stream_->pending_close_ = nullptr;
      stream_->closed_ = false;
      result_ = std::unexpected(submitted.error());
      return false;
    }

    return true;
  }

  base::Result<void> await_resume() noexcept {
    assert(result_.has_value());
    return std::move(*result_);
  }

  void TryComplete() noexcept {
    if (completed_ || stream_ == nullptr || !cancel_completed_) {
      return;
    }
    if (stream_->pending_read_ != nullptr || stream_->pending_write_ != nullptr) {
      return;
    }

    completed_ = true;
    LUringLoop* loop = stream_->loop_;
    stream_->pending_close_ = nullptr;
    result_ = CloseFd();
    stream_ = nullptr;
    loop->Schedule(&resume_work_);
  }

private:
  static void OnCancelComplete(LUringOp* op) noexcept {
    auto* self = static_cast<CloseAwaiter*>(op->owner);
    self->cancel_completed_ = true;
    self->TryComplete();
  }

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
  LUringOp cancel_op_{.kind = LUringOpKind::kClose};
  coro::ResumeWork resume_work_{};
  std::optional<base::Result<void>> result_;
  bool cancel_completed_{false};
  bool completed_{false};
};

LUringStream::LUringStream(LUringLoop* loop, int fd, net::InetAddress peer) noexcept
    : loop_(loop), fd_(fd), peer_(std::move(peer)) {
  assert(loop_ != nullptr);
  assert(fd_ >= 0);
}

LUringStream::~LUringStream() {
  assert(pending_read_ == nullptr);
  assert(pending_write_ == nullptr);
  assert(pending_close_ == nullptr);
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

coro::Task<base::Result<std::size_t>> LUringStream::ReadSome(std::span<std::byte> buffer) {
  co_return co_await ReadAwaiter(*this, buffer);
}

coro::Task<base::Result<std::size_t>> LUringStream::WriteSome(std::span<const std::byte> buffer) {
  co_return co_await WriteAwaiter(*this, buffer);
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

coro::Task<base::Result<void>> LUringStream::Close() {
  co_return co_await CloseAwaiter(*this);
}

void LUringStream::NotifyCloseProgress() noexcept {
  if (pending_close_ != nullptr) {
    pending_close_->TryComplete();
  }
}

}  // namespace vexo::luring
