// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/reactor/reactor_stream.h"

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
#include <span>
#include <utility>
#include <vector>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/work.h"
#include "coropact/net/net_utils.h"

namespace coropact::reactor {
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
  auto len = static_cast<socklen_t>(sizeof(err));
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
    result_.SetError(base::make_errno(EBADF));
    return false;
  }

  COROPACT_DCHECK(stream_->loop_->IsInLoopThread(), "ReadSomeAwaiter: wrong EventLoop thread");
  COROPACT_DCHECK(stream_->pending_read_ == nullptr,
              "ReadSomeAwaiter: only one pending read is supported per stream");

  scheduler_ = &coro::Scheduler::RequireCurrent();
  resume_work_.SetHandle(continuation);
  IoAttempt attempt = TryRead(stream_->socket_.fd(), buffer_);
  if (!attempt.pending) {
    result_.SetResult(attempt.result);
    return false;
  }

  stream_->pending_read_ = this;
  stream_->pending_read_kind_ = ReactorStream::PendingReadKind::kReadSome;
  if (!stream_->channel_.IsReading()) {
    stream_->channel_.EnableReading();
  }
  if (timeout_.count() > 0) {
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
  COROPACT_DCHECK(result_.HasResult(), "ReadSomeAwaiter: result is not ready");
  return result_.Take();
}

void ReactorStream::ReadSomeAwaiter::CompleteImpl(base::Result<std::size_t> result) noexcept {
  if (timer_.Valid()) {
    stream_->loop_->Cancel(timer_);
    timer_ = {};
  }
  stream_ = nullptr;
  result_.SetResult(result);
  COROPACT_DCHECK(scheduler_ != nullptr, "ReadSomeAwaiter: scheduler is not bound");
  scheduler_->Schedule(&resume_work_);
}

void ReactorStream::ReadSomeAwaiter::OnReadyImpl() noexcept {
  IoAttempt attempt = TryRead(stream_->socket_.fd(), buffer_);
  if (attempt.pending) {
    return;
  }
  stream_->CompleteRead(std::move(attempt.result));
}

ReactorStream::BufferReadAwaiter::BufferReadAwaiter(ReactorStream& stream, io::Buffer& buffer,
                                                    std::size_t reserve,
                                                    std::chrono::milliseconds timeout) noexcept
    : stream_(&stream),
      buffer_(&buffer),
      reserve_(std::max<std::size_t>(reserve, 1)),
      timeout_(timeout) {}

bool ReactorStream::BufferReadAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  if (stream_->closed_ || stream_->socket_.fd() < 0) {
    result_.SetError(base::make_errno(EBADF));
    return false;
  }

  COROPACT_DCHECK(stream_->loop_->IsInLoopThread(), "BufferReadAwaiter: wrong EventLoop thread");
  COROPACT_DCHECK(stream_->pending_read_ == nullptr,
              "BufferReadAwaiter: only one pending read is supported per stream");

  scheduler_ = &coro::Scheduler::RequireCurrent();
  resume_work_.SetHandle(continuation);

  if (!PrepareReservation()) {
    return false;
  }

  IoAttempt attempt = TryReadv(stream_->socket_.fd(), iovs_);
  if (!attempt.pending) {
    FinishAttempt(std::move(attempt.result));
    return false;
  }

  stream_->pending_read_ = this;
  stream_->pending_read_kind_ = ReactorStream::PendingReadKind::kBufferRead;
  if (!stream_->channel_.IsReading()) {
    stream_->channel_.EnableReading();
  }
  if (timeout_.count() > 0) {
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

base::Result<std::size_t> ReactorStream::BufferReadAwaiter::await_resume() noexcept {
  COROPACT_DCHECK(result_.HasResult(), "BufferReadAwaiter: result is not ready");
  return result_.Take();
}

void ReactorStream::BufferReadAwaiter::CompleteImpl(base::Result<std::size_t> result) noexcept {
  if (timer_.Valid()) {
    stream_->loop_->Cancel(timer_);
    timer_ = {};
  }
  FinishAttempt(std::move(result));
  stream_ = nullptr;
  COROPACT_DCHECK(scheduler_ != nullptr, "BufferReadAwaiter: scheduler is not bound");
  scheduler_->Schedule(&resume_work_);
}

void ReactorStream::BufferReadAwaiter::OnReadyImpl() noexcept {
  IoAttempt attempt = TryReadv(stream_->socket_.fd(), iovs_);
  if (attempt.pending) {
    return;
  }
  stream_->CompleteRead(std::move(attempt.result));
}

bool ReactorStream::BufferReadAwaiter::PrepareReservation() noexcept {
  try {
    iovs_ = buffer_->PrepareWrite(reserve_, 16);
  } catch (const std::bad_alloc&) {
    result_.SetError(base::make_errno(ENOMEM));
    return false;
  }

  if (iovs_.empty()) {
    result_.SetError(base::make_errno(ENOMEM));
    return false;
  }
  return true;
}

void ReactorStream::BufferReadAwaiter::FinishAttempt(base::Result<std::size_t> result) noexcept {
  if (result.has_value()) {
    buffer_->CommitWrite(*result);
  } else {
    buffer_->AbortWrite();
  }
  result_.SetResult(result);
}

bool ReactorStream::WriteSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (stream_->closed_ || stream_->socket_.fd() < 0) {
    result_.SetError(base::make_errno(EBADF));
    return false;
  }

  COROPACT_DCHECK(stream_->loop_->IsInLoopThread(), "WriteSomeAwaiter: wrong EventLoop thread");
  COROPACT_DCHECK(stream_->pending_write_ == nullptr,
              "WriteSomeAwaiter: only one pending write is supported per stream");

  scheduler_ = &coro::Scheduler::RequireCurrent();
  resume_work_.SetHandle(continuation);
  IoAttempt attempt = TryWrite(stream_->socket_.fd(), buffer_);
  if (!attempt.pending) {
    result_.SetResult(attempt.result);
    return false;
  }

  stream_->pending_write_ = this;
  stream_->pending_write_kind_ = ReactorStream::PendingWriteKind::kWriteSome;
  if (!stream_->channel_.IsWriting()) {
    stream_->channel_.EnableWriting();
  }
  return true;
}

base::Result<std::size_t> ReactorStream::WriteSomeAwaiter::await_resume() noexcept {
  COROPACT_DCHECK(result_.HasResult(), "WriteSomeAwaiter: result is not ready");
  return result_.Take();
}

void ReactorStream::WriteSomeAwaiter::CompleteImpl(base::Result<std::size_t> result) noexcept {
  result_.SetResult(result);
  COROPACT_DCHECK(scheduler_ != nullptr, "WriteSomeAwaiter: scheduler is not bound");
  scheduler_->Schedule(&resume_work_);
}

void ReactorStream::WriteSomeAwaiter::OnReadyImpl() noexcept {
  IoAttempt attempt = TryWrite(stream_->socket_.fd(), buffer_);
  if (attempt.pending) {
    return;
  }
  stream_->CompleteWrite(std::move(attempt.result));
}

ReactorStream::BufferWriteAwaiter::BufferWriteAwaiter(ReactorStream& stream,
                                                      io::Buffer& buffer) noexcept
    : stream_(&stream), buffer_(&buffer) {}

bool ReactorStream::BufferWriteAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  if (stream_->closed_ || stream_->socket_.fd() < 0) {
    result_.SetError(base::make_errno(EBADF));
    return false;
  }

  COROPACT_DCHECK(stream_->loop_->IsInLoopThread(), "BufferWriteAwaiter: wrong EventLoop thread");
  COROPACT_DCHECK(stream_->pending_write_ == nullptr,
              "BufferWriteAwaiter: only one pending write is supported per stream");

  scheduler_ = &coro::Scheduler::RequireCurrent();
  resume_work_.SetHandle(continuation);

  if (!PrepareReadable()) {
    return false;
  }

  IoAttempt attempt = TryWritev(stream_->socket_.fd(), iovs_);
  if (!attempt.pending) {
    FinishAttempt(std::move(attempt.result));
    return false;
  }

  stream_->pending_write_ = this;
  stream_->pending_write_kind_ = ReactorStream::PendingWriteKind::kBufferWrite;
  if (!stream_->channel_.IsWriting()) {
    stream_->channel_.EnableWriting();
  }
  return true;
}

base::Result<std::size_t> ReactorStream::BufferWriteAwaiter::await_resume() noexcept {
  COROPACT_DCHECK(result_.HasResult(), "BufferWriteAwaiter: result is not ready");
  return result_.Take();
}

void ReactorStream::BufferWriteAwaiter::CompleteImpl(base::Result<std::size_t> result) noexcept {
  FinishAttempt(std::move(result));
  COROPACT_DCHECK(scheduler_ != nullptr, "BufferWriteAwaiter: scheduler is not bound");
  scheduler_->Schedule(&resume_work_);
}

void ReactorStream::BufferWriteAwaiter::OnReadyImpl() noexcept {
  IoAttempt attempt = TryWritev(stream_->socket_.fd(), iovs_);
  if (attempt.pending) {
    return;
  }
  stream_->CompleteWrite(std::move(attempt.result));
}

bool ReactorStream::BufferWriteAwaiter::PrepareReadable() noexcept {
  if (buffer_->Empty()) {
    result_.SetSuccess(0);
    return false;
  }

  try {
    iovs_ = buffer_->ReadableIov(16);
  } catch (const std::bad_alloc&) {
    result_.SetError(base::make_errno(ENOMEM));
    return false;
  }

  if (iovs_.empty()) {
    result_.SetSuccess(0);
    return false;
  }
  return true;
}

void ReactorStream::BufferWriteAwaiter::FinishAttempt(base::Result<std::size_t> result) noexcept {
  if (result.has_value() && *result > 0) {
    buffer_->Drain(*result);
  }
  result_.SetResult(result);
}

ReactorStream::ReactorStream(EventLoop* loop, int fd, net::Endpoint peer)
    : loop_(loop), socket_(fd), channel_(loop, fd), peer_(peer) {
  COROPACT_DCHECK(loop_ != nullptr, "ReactorStream: loop must not be null");
  [[maybe_unused]] auto nonblocking = net::set_non_blocking(fd, true);
  COROPACT_DCHECK(nonblocking.has_value(), "ReactorStream: failed to set non-blocking mode");

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
  COROPACT_CHECK(loop_ == nullptr || loop_ == other_loop,
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
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream destructor called from wrong thread");
  COROPACT_DCHECK(pending_read_ == nullptr, "ReactorStream destroyed with a pending read");
  COROPACT_DCHECK(pending_write_ == nullptr, "ReactorStream destroyed with a pending write");
  DetachChannel();
}

ReactorStream::ReadSomeAwaiter ReactorStream::ReadSome(std::span<std::byte> buffer) noexcept {
  return ReadSomeAwaiter(*this, buffer);
}

ReactorStream::BufferReadAwaiter ReactorStream::ReadSome(io::Buffer& buffer,
                                                         std::size_t reserve) noexcept {
  return BufferReadAwaiter(*this, buffer, reserve);
}

ReactorStream::ReadSomeAwaiter ReactorStream::ReadSomeFor(
    std::span<std::byte> buffer, std::chrono::milliseconds timeout) noexcept {
  return ReadSomeAwaiter(*this, buffer, timeout);
}

ReactorStream::BufferReadAwaiter ReactorStream::ReadSomeFor(io::Buffer& buffer,
                                                            std::chrono::milliseconds timeout,
                                                            std::size_t reserve) noexcept {
  return BufferReadAwaiter(*this, buffer, reserve, timeout);
}

ReactorStream::WriteSomeAwaiter ReactorStream::WriteSome(
    std::span<const std::byte> buffer) noexcept {
  return WriteSomeAwaiter(*this, buffer);
}

ReactorStream::BufferWriteAwaiter ReactorStream::WriteSome(io::Buffer& buffer) noexcept {
  return BufferWriteAwaiter(*this, buffer);
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

void ReactorStream::HandleRead(coropact::time::Timestamp /*receive_time*/) {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::HandleRead called from wrong thread");
  if (pending_read_ == nullptr) {
    return;
  }
  switch (pending_read_kind_) {
    case PendingReadKind::kReadSome:
      static_cast<ReadSomeAwaiter*>(pending_read_)->OnReady();
      return;
    case PendingReadKind::kBufferRead:
      static_cast<BufferReadAwaiter*>(pending_read_)->OnReady();
      return;
    case PendingReadKind::kNone:
      return;
  }
}

void ReactorStream::HandleWrite() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::HandleWrite called from wrong thread");
  if (pending_write_ == nullptr) {
    return;
  }
  switch (pending_write_kind_) {
    case PendingWriteKind::kWriteSome:
      static_cast<WriteSomeAwaiter*>(pending_write_)->OnReady();
      return;
    case PendingWriteKind::kBufferWrite:
      static_cast<BufferWriteAwaiter*>(pending_write_)->OnReady();
      return;
    case PendingWriteKind::kNone:
      return;
  }
}

void ReactorStream::HandleClose() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::HandleClose called from wrong thread");
  CompleteRead(base::Result<std::size_t>{0});
  CompleteWrite(std::unexpected(base::make_errno(EPIPE)));
}

void ReactorStream::HandleError() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::HandleError called from wrong thread");
  base::Error error = SocketError(socket_.fd());
  CompleteRead(std::unexpected(error));
  CompleteWrite(std::unexpected(error));
}

void ReactorStream::CompleteRead(base::Result<std::size_t> result) {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::CompleteRead called from wrong thread");
  void* awaiter = std::exchange(pending_read_, nullptr);
  const PendingReadKind kind = std::exchange(pending_read_kind_, PendingReadKind::kNone);
  if (awaiter == nullptr) {
    return;
  }
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  switch (kind) {
    case PendingReadKind::kReadSome:
      static_cast<ReadSomeAwaiter*>(awaiter)->Complete(std::move(result));
      return;
    case PendingReadKind::kBufferRead:
      static_cast<BufferReadAwaiter*>(awaiter)->Complete(std::move(result));
      return;
    case PendingReadKind::kNone:
      COROPACT_CHECK(false, "ReactorStream::CompleteRead missing operation kind");
      return;
  }
}

void ReactorStream::CompleteWrite(base::Result<std::size_t> result) {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::CompleteWrite called from wrong thread");
  void* awaiter = std::exchange(pending_write_, nullptr);
  const PendingWriteKind kind = std::exchange(pending_write_kind_, PendingWriteKind::kNone);
  if (awaiter == nullptr) {
    return;
  }
  if (channel_.IsWriting()) {
    channel_.DisableWriting();
  }
  switch (kind) {
    case PendingWriteKind::kWriteSome:
      static_cast<WriteSomeAwaiter*>(awaiter)->Complete(std::move(result));
      return;
    case PendingWriteKind::kBufferWrite:
      static_cast<BufferWriteAwaiter*>(awaiter)->Complete(std::move(result));
      return;
    case PendingWriteKind::kNone:
      COROPACT_CHECK(false, "ReactorStream::CompleteWrite missing operation kind");
      return;
  }
}

void ReactorStream::DetachChannel() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::DetachChannel called from wrong thread");
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (loop_->HasChannel(&channel_)) {
    channel_.Remove();
  }
}

void ReactorStream::DispatchRead(void* context,
                                 coropact::time::Timestamp receive_time) noexcept {
  static_cast<ReactorStream*>(context)->HandleRead(receive_time);
}

void ReactorStream::DispatchWrite(void* context) noexcept {
  static_cast<ReactorStream*>(context)->HandleWrite();
}

void ReactorStream::DispatchClose(void* context) noexcept {
  static_cast<ReactorStream*>(context)->HandleClose();
}

void ReactorStream::DispatchError(void* context) noexcept {
  static_cast<ReactorStream*>(context)->HandleError();
}

void ReactorStream::BindChannelCallbacks() noexcept {
  try {
    channel_.SetReadCallback(&ReactorStream::DispatchRead, this);
    channel_.SetWriteCallback(&ReactorStream::DispatchWrite, this);
    channel_.SetCloseCallback(&ReactorStream::DispatchClose, this);
    channel_.SetErrorCallback(&ReactorStream::DispatchError, this);
  } catch (...) {
    COROPACT_CHECK(false, "ReactorStream: failed to bind channel callbacks");
  }
}

void ReactorStream::ResetForMove() noexcept {
  COROPACT_CHECK(loop_ != nullptr, "ReactorStream move destination is not initialized");
  COROPACT_CHECK(loop_->IsInLoopThread(), "ReactorStream move called from wrong EventLoop thread");
  COROPACT_CHECK(pending_read_ == nullptr, "ReactorStream move destination has a pending read");
  COROPACT_CHECK(pending_write_ == nullptr, "ReactorStream move destination has a pending write");
  DetachChannel();
  socket_.Close();
}

EventLoop* ReactorStream::PrepareMove(ReactorStream& other) noexcept {
  COROPACT_CHECK(other.loop_ != nullptr, "ReactorStream move source is not initialized");
  COROPACT_CHECK(other.loop_->IsInLoopThread(),
             "ReactorStream move called from wrong EventLoop thread");
  COROPACT_CHECK(other.pending_read_ == nullptr,
             "ReactorStream cannot move with a pending read operation");
  COROPACT_CHECK(other.pending_write_ == nullptr,
             "ReactorStream cannot move with a pending write operation");

  other.DetachChannel();
  EventLoop* loop = std::exchange(other.loop_, nullptr);
  return loop;
}

}  // namespace coropact::reactor
