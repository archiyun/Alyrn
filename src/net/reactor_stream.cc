// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/reactor_stream.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <coroutine>
#include <cstddef>
#include <expected>
#include <optional>
#include <span>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/work.h"
#include "vexo/net/net_utils.h"

namespace vexo::net {
namespace {

vexo::base::Error CurrentErrno() noexcept { return vexo::base::make_errno(errno); }

bool IsWouldBlock(int err) noexcept { return err == EAGAIN || err == EWOULDBLOCK; }

struct IoAttempt {
  bool pending{false};
  base::Result<std::size_t> result{0};
};

IoAttempt TryRead(int fd, std::span<std::byte> buffer) noexcept {
  while (true) {
    const ssize_t n = ::read(fd, buffer.data(), buffer.size());
    if (n >= 0) {
      return {.pending = false, .result = static_cast<std::size_t>(n)};
    }

    const int err = errno;
    if (err == EINTR) {
      continue;
    }
    if (IsWouldBlock(err)) {
      return {.pending = true, .result = 0};
    }
    return {.pending = false, .result = std::unexpected(base::make_errno(err))};
  }
}

IoAttempt TryWrite(int fd, std::span<const std::byte> buffer) noexcept {
  while (true) {
    const ssize_t n = ::write(fd, buffer.data(), buffer.size());
    if (n >= 0) {
      return {.pending = false, .result = static_cast<std::size_t>(n)};
    }

    const int err = errno;
    if (err == EINTR) {
      continue;
    }
    if (IsWouldBlock(err)) {
      return {.pending = true, .result = 0};
    }
    return {.pending = false, .result = std::unexpected(base::make_errno(err))};
  }
}

base::Error SocketError(int fd) noexcept {
  int err = 0;
  socklen_t len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    return CurrentErrno();
  }
  if (err == 0) {
    err = EIO;
  }
  return base::make_errno(err);
}

}  // namespace

class ReactorStream::ReadAwaiter {
public:
  ReadAwaiter(ReactorStream& stream, std::span<std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    assert(stream_->loop_->IsInLoopThread());
    assert(stream_->pending_read_ == nullptr && "only one pending read is supported per stream");

    scheduler_ = &coro::Scheduler::RequireCurrent();
    resume_work_.handle = continuation;
    IoAttempt attempt = TryRead(stream_->socket_.fd(), buffer_);
    if (!attempt.pending) {
      result_ = std::move(attempt.result);
      return false;
    }

    stream_->pending_read_ = this;
    if (!stream_->channel_.IsReading()) {
      stream_->channel_.EnableReading();
    }
    return true;
  }

  base::Result<std::size_t> await_resume() noexcept {
    assert(result_.has_value());
    return std::move(*result_);
  }

  void Complete(base::Result<std::size_t> result) noexcept {
    result_ = std::move(result);
    assert(scheduler_ != nullptr);
    scheduler_->Schedule(&resume_work_);
  }

  void OnReady() noexcept {
    IoAttempt attempt = TryRead(stream_->socket_.fd(), buffer_);
    if (attempt.pending) {
      return;
    }
    stream_->CompleteRead(std::move(attempt.result));
  }

private:
  ReactorStream* stream_;
  std::span<std::byte> buffer_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_{};
  std::optional<base::Result<std::size_t>> result_;
};

class ReactorStream::WriteAwaiter {
public:
  WriteAwaiter(ReactorStream& stream, std::span<const std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    assert(stream_->loop_->IsInLoopThread());
    assert(stream_->pending_write_ == nullptr && "only one pending write is supported per stream");

    scheduler_ = &coro::Scheduler::RequireCurrent();
    resume_work_.handle = continuation;
    IoAttempt attempt = TryWrite(stream_->socket_.fd(), buffer_);
    if (!attempt.pending) {
      result_ = std::move(attempt.result);
      return false;
    }

    stream_->pending_write_ = this;
    if (!stream_->channel_.IsWriting()) {
      stream_->channel_.EnableWriting();
    }
    return true;
  }

  base::Result<std::size_t> await_resume() noexcept {
    assert(result_.has_value());
    return std::move(*result_);
  }

  void Complete(base::Result<std::size_t> result) noexcept {
    result_ = std::move(result);
    assert(scheduler_ != nullptr);
    scheduler_->Schedule(&resume_work_);
  }

  void OnReady() noexcept {
    IoAttempt attempt = TryWrite(stream_->socket_.fd(), buffer_);
    if (attempt.pending) {
      return;
    }
    stream_->CompleteWrite(std::move(attempt.result));
  }

private:
  ReactorStream* stream_;
  std::span<const std::byte> buffer_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_{};
  std::optional<base::Result<std::size_t>> result_;
};

ReactorStream::ReactorStream(EventLoop* loop, int fd)
    : loop_(loop), socket_(fd), channel_(loop, fd) {
  assert(loop_ != nullptr);
  [[maybe_unused]] auto nonblocking = set_non_blocking(fd, true);
  assert(nonblocking.has_value());

  channel_.set_read_callback([this](vexo::time::Timestamp ts) { HandleRead(ts); });
  channel_.set_write_callback([this] { HandleWrite(); });
  channel_.set_close_callback([this] { HandleClose(); });
  channel_.set_error_callback([this] { HandleError(); });
}

ReactorStream::~ReactorStream() {
  assert(loop_->IsInLoopThread());
  assert(pending_read_ == nullptr);
  assert(pending_write_ == nullptr);
  DetachChannel();
}

coro::Task<base::Result<std::size_t>> ReactorStream::ReadSome(std::span<std::byte> buffer) {
  if (closed_) {
    co_return std::unexpected(base::make_errno(EBADF));
  }
  co_return co_await ReadAwaiter(*this, buffer);
}

coro::Task<base::Result<std::size_t>> ReactorStream::WriteSome(std::span<const std::byte> buffer) {
  if (closed_) {
    co_return std::unexpected(base::make_errno(EBADF));
  }
  co_return co_await WriteAwaiter(*this, buffer);
}

coro::Task<base::Result<void>> ReactorStream::Shutdown() {
  if (closed_) {
    co_return std::unexpected(base::make_errno(EBADF));
  }
  socket_.ShutdownWrite();
  co_return base::Result<void>{};
}

coro::Task<base::Result<void>> ReactorStream::Close() {
  if (closed_) {
    co_return base::Result<void>{};
  }

  closed_ = true;
  if (pending_read_ != nullptr) {
    CompleteRead(std::unexpected(base::make_errno(ECANCELED)));
  }
  if (pending_write_ != nullptr) {
    CompleteWrite(std::unexpected(base::make_errno(ECANCELED)));
  }
  DetachChannel();
  socket_.Close();
  co_return base::Result<void>{};
}

void ReactorStream::HandleRead(vexo::time::Timestamp /*receive_time*/) {
  assert(loop_->IsInLoopThread());
  if (pending_read_ != nullptr) {
    pending_read_->OnReady();
  }
}

void ReactorStream::HandleWrite() {
  assert(loop_->IsInLoopThread());
  if (pending_write_ != nullptr) {
    pending_write_->OnReady();
  }
}

void ReactorStream::HandleClose() {
  assert(loop_->IsInLoopThread());
  CompleteRead(base::Result<std::size_t>{0});
  CompleteWrite(std::unexpected(base::make_errno(EPIPE)));
}

void ReactorStream::HandleError() {
  assert(loop_->IsInLoopThread());
  base::Error error = SocketError(socket_.fd());
  CompleteRead(std::unexpected(error));
  CompleteWrite(std::unexpected(error));
}

void ReactorStream::CompleteRead(base::Result<std::size_t> result) {
  assert(loop_->IsInLoopThread());
  ReadAwaiter* awaiter = std::exchange(pending_read_, nullptr);
  if (awaiter == nullptr) {
    return;
  }
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  awaiter->Complete(std::move(result));
}

void ReactorStream::CompleteWrite(base::Result<std::size_t> result) {
  assert(loop_->IsInLoopThread());
  WriteAwaiter* awaiter = std::exchange(pending_write_, nullptr);
  if (awaiter == nullptr) {
    return;
  }
  if (channel_.IsWriting()) {
    channel_.DisableWriting();
  }
  awaiter->Complete(std::move(result));
}

void ReactorStream::DetachChannel() {
  assert(loop_->IsInLoopThread());
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (loop_->HasChannel(&channel_)) {
    channel_.Remove();
  }
}

}  // namespace vexo::net
