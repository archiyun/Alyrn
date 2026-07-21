// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/reactor_stream.h"

#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <expected>
#include <new>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "vexo/base/check.h"
#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/work.h"
#include "vexo/net/net_utils.h"

namespace vexo::net {
namespace {

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

IoAttempt TryReadv(int fd, const std::vector<iovec>& iovs) noexcept {
  if (iovs.empty()) {
    return {.pending = false, .result = 0};
  }

  while (true) {
    const ssize_t n = ::readv(fd, iovs.data(), static_cast<int>(iovs.size()));
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

IoAttempt TryWritev(int fd, const std::vector<iovec>& iovs) noexcept {
  if (iovs.empty()) {
    return {.pending = false, .result = 0};
  }

  while (true) {
    const ssize_t n = ::writev(fd, iovs.data(), static_cast<int>(iovs.size()));
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
    return base::CurrentErrno();
  }
  if (err == 0) {
    err = EIO;
  }
  return base::make_errno(err);
}

}  // namespace

bool ReactorStream::ReadSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (stream_->closed_ || stream_->socket_.fd() < 0) {
    result_ = std::unexpected(base::make_errno(EBADF));
    return false;
  }

  VEXO_DCHECK(stream_->loop_->IsInLoopThread(), "ReadSomeAwaiter: wrong EventLoop thread");
  VEXO_DCHECK(stream_->pending_read_ == nullptr,
              "ReadSomeAwaiter: only one pending read is supported per stream");

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
  if (timeout_.count() > 0) {
    timer_armed_ = true;
    const auto seconds =
        std::chrono::duration<double>(std::max(timeout_, std::chrono::milliseconds{1})).count();
    timer_ = stream_->loop_->RunAfter(seconds, [this] {
      if (stream_ != nullptr && stream_->pending_read_ == this) {
        stream_->CompleteRead(std::unexpected(base::make_errno(ETIMEDOUT)));
      }
    });
  }
  return true;
}

base::Result<std::size_t> ReactorStream::ReadSomeAwaiter::await_resume() noexcept {
  VEXO_DCHECK(result_.has_value(), "ReadSomeAwaiter: result is not ready");
  return std::move(*result_);
}

void ReactorStream::ReadSomeAwaiter::Complete(base::Result<std::size_t> result) noexcept {
  if (timer_armed_) {
    timer_armed_ = false;
    stream_->loop_->Cancel(timer_);
  }
  stream_ = nullptr;
  result_ = std::move(result);
  VEXO_DCHECK(scheduler_ != nullptr, "ReadSomeAwaiter: scheduler is not bound");
  scheduler_->Schedule(&resume_work_);
}

void ReactorStream::ReadSomeAwaiter::OnReady() noexcept {
  IoAttempt attempt = TryRead(stream_->socket_.fd(), buffer_);
  if (attempt.pending) {
    return;
  }
  stream_->CompleteRead(std::move(attempt.result));
}

class ReactorStream::BufferReadAwaiter : public ReactorStream::ReadOperation {
public:
  BufferReadAwaiter(ReactorStream& stream, io::Buffer& buffer, std::size_t reserve,
                    std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) noexcept
      : stream_(&stream),
        buffer_(&buffer),
        reserve_(std::max<std::size_t>(reserve, 1)),
        timeout_(timeout) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    VEXO_DCHECK(stream_->loop_->IsInLoopThread(), "BufferReadAwaiter: wrong EventLoop thread");
    VEXO_DCHECK(stream_->pending_read_ == nullptr,
                "BufferReadAwaiter: only one pending read is supported per stream");

    scheduler_ = &coro::Scheduler::RequireCurrent();
    resume_work_.handle = continuation;

    if (!PrepareReservation()) {
      return false;
    }

    IoAttempt attempt = TryReadv(stream_->socket_.fd(), iovs_);
    if (!attempt.pending) {
      FinishAttempt(std::move(attempt.result));
      return false;
    }

    stream_->pending_read_ = this;
    if (!stream_->channel_.IsReading()) {
      stream_->channel_.EnableReading();
    }
    if (timeout_.count() > 0) {
      timer_armed_ = true;
      const auto seconds =
          std::chrono::duration<double>(std::max(timeout_, std::chrono::milliseconds{1})).count();
      timer_ = stream_->loop_->RunAfter(seconds, [this] {
        if (stream_ != nullptr && stream_->pending_read_ == this) {
          stream_->CompleteRead(std::unexpected(base::make_errno(ETIMEDOUT)));
        }
      });
    }
    return true;
  }

  base::Result<std::size_t> await_resume() noexcept {
    VEXO_DCHECK(result_.has_value(), "BufferReadAwaiter: result is not ready");
    return std::move(*result_);
  }

  void Complete(base::Result<std::size_t> result) noexcept override {
    if (timer_armed_) {
      timer_armed_ = false;
      stream_->loop_->Cancel(timer_);
    }
    FinishAttempt(std::move(result));
    stream_ = nullptr;
    VEXO_DCHECK(scheduler_ != nullptr, "BufferReadAwaiter: scheduler is not bound");
    scheduler_->Schedule(&resume_work_);
  }

  void OnReady() noexcept override {
    IoAttempt attempt = TryReadv(stream_->socket_.fd(), iovs_);
    if (attempt.pending) {
      return;
    }
    stream_->CompleteRead(std::move(attempt.result));
  }

private:
  bool PrepareReservation() noexcept {
    try {
      iovs_ = buffer_->PrepareWrite(reserve_, 16);
    } catch (const std::bad_alloc&) {
      result_ = std::unexpected(base::make_errno(ENOMEM));
      return false;
    }

    if (iovs_.empty()) {
      result_ = std::unexpected(base::make_errno(ENOMEM));
      return false;
    }
    return true;
  }

  void FinishAttempt(base::Result<std::size_t> result) noexcept {
    if (result.has_value()) {
      buffer_->CommitWrite(*result);
    } else {
      buffer_->AbortWrite();
    }
    result_ = std::move(result);
  }

  ReactorStream* stream_;
  io::Buffer* buffer_;
  std::size_t reserve_;
  std::chrono::milliseconds timeout_;
  std::vector<iovec> iovs_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_{};
  std::optional<base::Result<std::size_t>> result_;
  vexo::time::TimerId timer_;
  bool timer_armed_{false};
};

bool ReactorStream::WriteSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (stream_->closed_ || stream_->socket_.fd() < 0) {
    result_ = std::unexpected(base::make_errno(EBADF));
    return false;
  }

  VEXO_DCHECK(stream_->loop_->IsInLoopThread(), "WriteSomeAwaiter: wrong EventLoop thread");
  VEXO_DCHECK(stream_->pending_write_ == nullptr,
              "WriteSomeAwaiter: only one pending write is supported per stream");

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

base::Result<std::size_t> ReactorStream::WriteSomeAwaiter::await_resume() noexcept {
  VEXO_DCHECK(result_.has_value(), "WriteSomeAwaiter: result is not ready");
  return std::move(*result_);
}

void ReactorStream::WriteSomeAwaiter::Complete(base::Result<std::size_t> result) noexcept {
  result_ = std::move(result);
  VEXO_DCHECK(scheduler_ != nullptr, "WriteSomeAwaiter: scheduler is not bound");
  scheduler_->Schedule(&resume_work_);
}

void ReactorStream::WriteSomeAwaiter::OnReady() noexcept {
  IoAttempt attempt = TryWrite(stream_->socket_.fd(), buffer_);
  if (attempt.pending) {
    return;
  }
  stream_->CompleteWrite(std::move(attempt.result));
}

class ReactorStream::BufferWriteAwaiter : public ReactorStream::WriteOperation {
public:
  BufferWriteAwaiter(ReactorStream& stream, io::Buffer& buffer) noexcept
      : stream_(&stream), buffer_(&buffer) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    VEXO_DCHECK(stream_->loop_->IsInLoopThread(), "BufferWriteAwaiter: wrong EventLoop thread");
    VEXO_DCHECK(stream_->pending_write_ == nullptr,
                "BufferWriteAwaiter: only one pending write is supported per stream");

    scheduler_ = &coro::Scheduler::RequireCurrent();
    resume_work_.handle = continuation;

    if (!PrepareReadable()) {
      return false;
    }

    IoAttempt attempt = TryWritev(stream_->socket_.fd(), iovs_);
    if (!attempt.pending) {
      FinishAttempt(std::move(attempt.result));
      return false;
    }

    stream_->pending_write_ = this;
    if (!stream_->channel_.IsWriting()) {
      stream_->channel_.EnableWriting();
    }
    return true;
  }

  base::Result<std::size_t> await_resume() noexcept {
    VEXO_DCHECK(result_.has_value(), "BufferWriteAwaiter: result is not ready");
    return std::move(*result_);
  }

  void Complete(base::Result<std::size_t> result) noexcept override {
    FinishAttempt(std::move(result));
    VEXO_DCHECK(scheduler_ != nullptr, "BufferWriteAwaiter: scheduler is not bound");
    scheduler_->Schedule(&resume_work_);
  }

  void OnReady() noexcept override {
    IoAttempt attempt = TryWritev(stream_->socket_.fd(), iovs_);
    if (attempt.pending) {
      return;
    }
    stream_->CompleteWrite(std::move(attempt.result));
  }

private:
  bool PrepareReadable() noexcept {
    if (buffer_->Empty()) {
      result_ = base::Result<std::size_t>{0};
      return false;
    }

    try {
      iovs_ = buffer_->ReadableIov(16);
    } catch (const std::bad_alloc&) {
      result_ = std::unexpected(base::make_errno(ENOMEM));
      return false;
    }

    if (iovs_.empty()) {
      result_ = base::Result<std::size_t>{0};
      return false;
    }
    return true;
  }

  void FinishAttempt(base::Result<std::size_t> result) noexcept {
    if (result.has_value() && *result > 0) {
      buffer_->Drain(*result);
    }
    result_ = std::move(result);
  }

  ReactorStream* stream_;
  io::Buffer* buffer_;
  std::vector<iovec> iovs_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_{};
  std::optional<base::Result<std::size_t>> result_;
};

ReactorStream::ReactorStream(EventLoop* loop, int fd, InetAddress peer)
    : loop_(loop), socket_(fd), channel_(loop, fd), peer_(std::move(peer)) {
  VEXO_DCHECK(loop_ != nullptr, "ReactorStream: loop must not be null");
  [[maybe_unused]] auto nonblocking = set_non_blocking(fd, true);
  VEXO_DCHECK(nonblocking.has_value(), "ReactorStream: failed to set non-blocking mode");

  BindChannelCallbacks();
}

ReactorStream::ReactorStream(ReactorStream&& other) noexcept
    : loop_(PrepareMove(other)),
      socket_(std::move(other.socket_)),
      channel_(std::move(other.channel_)),
      peer_(std::move(other.peer_)),
      pending_read_(nullptr),
      pending_write_(nullptr),
      closed_(other.closed_) {
  BindChannelCallbacks();
  other.closed_ = true;
}

ReactorStream& ReactorStream::operator=(ReactorStream&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  EventLoop* other_loop = PrepareMove(other);
  VEXO_CHECK(loop_ == nullptr || loop_ == other_loop,
             "ReactorStream move requires both objects to use the same EventLoop");
  if (loop_ != nullptr) {
    ResetForMove();
  }

  loop_ = other_loop;
  socket_ = std::move(other.socket_);
  channel_ = std::move(other.channel_);
  peer_ = std::move(other.peer_);
  pending_read_ = nullptr;
  pending_write_ = nullptr;
  closed_ = other.closed_;
  BindChannelCallbacks();
  other.closed_ = true;
  return *this;
}

ReactorStream::~ReactorStream() {
  if (loop_ == nullptr) {
    return;
  }
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorStream destructor called from wrong thread");
  VEXO_DCHECK(pending_read_ == nullptr, "ReactorStream destroyed with a pending read");
  VEXO_DCHECK(pending_write_ == nullptr, "ReactorStream destroyed with a pending write");
  DetachChannel();
}

ReactorStream::ReadSomeAwaiter ReactorStream::ReadSome(std::span<std::byte> buffer) noexcept {
  return ReadSomeAwaiter(*this, buffer);
}

coro::Task<base::Result<std::size_t>> ReactorStream::ReadSome(io::Buffer& buffer,
                                                              std::size_t reserve) {
  if (closed_) {
    co_return std::unexpected(base::make_errno(EBADF));
  }
  co_return co_await BufferReadAwaiter(*this, buffer, reserve);
}

coro::Task<base::Result<std::size_t>> ReactorStream::ReadSomeFor(
    std::span<std::byte> buffer, std::chrono::milliseconds timeout) {
  if (closed_) {
    co_return std::unexpected(base::make_errno(EBADF));
  }
  co_return co_await ReadSomeAwaiter(*this, buffer, timeout);
}

coro::Task<base::Result<std::size_t>> ReactorStream::ReadSomeFor(io::Buffer& buffer,
                                                                 std::chrono::milliseconds timeout,
                                                                 std::size_t reserve) {
  if (closed_) {
    co_return std::unexpected(base::make_errno(EBADF));
  }
  co_return co_await BufferReadAwaiter(*this, buffer, reserve, timeout);
}

ReactorStream::WriteSomeAwaiter ReactorStream::WriteSome(
    std::span<const std::byte> buffer) noexcept {
  return WriteSomeAwaiter(*this, buffer);
}

coro::Task<base::Result<std::size_t>> ReactorStream::WriteSome(io::Buffer& buffer) {
  if (closed_) {
    co_return std::unexpected(base::make_errno(EBADF));
  }
  co_return co_await BufferWriteAwaiter(*this, buffer);
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
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorStream::HandleRead called from wrong thread");
  if (pending_read_ != nullptr) {
    pending_read_->OnReady();
  }
}

void ReactorStream::HandleWrite() {
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorStream::HandleWrite called from wrong thread");
  if (pending_write_ != nullptr) {
    pending_write_->OnReady();
  }
}

void ReactorStream::HandleClose() {
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorStream::HandleClose called from wrong thread");
  CompleteRead(base::Result<std::size_t>{0});
  CompleteWrite(std::unexpected(base::make_errno(EPIPE)));
}

void ReactorStream::HandleError() {
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorStream::HandleError called from wrong thread");
  base::Error error = SocketError(socket_.fd());
  CompleteRead(std::unexpected(error));
  CompleteWrite(std::unexpected(error));
}

void ReactorStream::CompleteRead(base::Result<std::size_t> result) {
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorStream::CompleteRead called from wrong thread");
  ReadOperation* awaiter = std::exchange(pending_read_, nullptr);
  if (awaiter == nullptr) {
    return;
  }
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  awaiter->Complete(std::move(result));
}

void ReactorStream::CompleteWrite(base::Result<std::size_t> result) {
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorStream::CompleteWrite called from wrong thread");
  WriteOperation* awaiter = std::exchange(pending_write_, nullptr);
  if (awaiter == nullptr) {
    return;
  }
  if (channel_.IsWriting()) {
    channel_.DisableWriting();
  }
  awaiter->Complete(std::move(result));
}

void ReactorStream::DetachChannel() {
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorStream::DetachChannel called from wrong thread");
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (loop_->HasChannel(&channel_)) {
    channel_.Remove();
  }
}

void ReactorStream::BindChannelCallbacks() noexcept {
  try {
    channel_.set_read_callback([this](vexo::time::Timestamp ts) { HandleRead(ts); });
    channel_.set_write_callback([this] { HandleWrite(); });
    channel_.set_close_callback([this] { HandleClose(); });
    channel_.set_error_callback([this] { HandleError(); });
  } catch (...) {
    VEXO_CHECK(false, "ReactorStream: failed to bind channel callbacks");
  }
}

void ReactorStream::ResetForMove() noexcept {
  VEXO_CHECK(loop_ != nullptr, "ReactorStream move destination is not initialized");
  VEXO_CHECK(loop_->IsInLoopThread(), "ReactorStream move called from wrong EventLoop thread");
  VEXO_CHECK(pending_read_ == nullptr, "ReactorStream move destination has a pending read");
  VEXO_CHECK(pending_write_ == nullptr, "ReactorStream move destination has a pending write");
  DetachChannel();
  socket_.Close();
}

EventLoop* ReactorStream::PrepareMove(ReactorStream& other) noexcept {
  VEXO_CHECK(other.loop_ != nullptr, "ReactorStream move source is not initialized");
  VEXO_CHECK(other.loop_->IsInLoopThread(),
             "ReactorStream move called from wrong EventLoop thread");
  VEXO_CHECK(other.pending_read_ == nullptr,
             "ReactorStream cannot move with a pending read operation");
  VEXO_CHECK(other.pending_write_ == nullptr,
             "ReactorStream cannot move with a pending write operation");

  other.DetachChannel();
  EventLoop* loop = other.loop_;
  other.loop_ = nullptr;
  return loop;
}

}  // namespace vexo::net
