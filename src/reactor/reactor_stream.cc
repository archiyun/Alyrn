// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <new>
#include <span>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/net/net_utils.h"
#include "coropact/reactor/detail/loop_access.h"
#include "coropact/reactor/stream.h"

namespace coropact::reactor {

using detail::LoopAccess;

namespace {

[[nodiscard]]
constexpr bool IsWouldBlock(int error) noexcept {
  return error == EAGAIN || error == EWOULDBLOCK;
}

enum class IoAttemptState : std::uint8_t {
  kCompleted,
  kWouldBlock,
};

struct IoAttempt {
  IoAttemptState state{IoAttemptState::kCompleted};
  base::Result<std::size_t> result{0};

  [[nodiscard]]
  static IoAttempt Completed(std::size_t bytes) noexcept {
    return {
        .state = IoAttemptState::kCompleted,
        .result = bytes,
    };
  }

  [[nodiscard]]
  static IoAttempt WouldBlock() noexcept {
    return {
        .state = IoAttemptState::kWouldBlock,
        .result = std::size_t{0},
    };
  }

  [[nodiscard]]
  static IoAttempt Failed(base::Error error) noexcept {
    return {
        .state = IoAttemptState::kCompleted,
        .result = std::unexpected(error),
    };
  }

  [[nodiscard]]
  bool Pending() const noexcept {
    return state == IoAttemptState::kWouldBlock;
  }
};

template <typename Operation>
[[nodiscard]]
IoAttempt RetryNonBlockingIo(Operation&& operation) noexcept {
  while (true) {
    const ssize_t result = operation();
    if (result >= 0) {
      return IoAttempt::Completed(static_cast<std::size_t>(result));
    }

    const int error = errno;
    if (error == EINTR) {
      continue;
    }
    if (IsWouldBlock(error)) {
      return IoAttempt::WouldBlock();
    }
    return IoAttempt::Failed(base::MakeErrno(error));
  }
}

[[nodiscard]]
IoAttempt TryRead(int fd, std::span<std::byte> buffer) noexcept {
  return RetryNonBlockingIo(
      [fd, buffer]() noexcept { return ::read(fd, buffer.data(), buffer.size()); });
}

[[nodiscard]]
IoAttempt TryWrite(int fd, std::span<const std::byte> buffer) noexcept {
  return RetryNonBlockingIo(
      [fd, buffer]() noexcept { return ::write(fd, buffer.data(), buffer.size()); });
}

[[nodiscard]]
base::Result<int> CheckedIovCount(std::size_t count) noexcept {
  if (count > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }

#if defined(IOV_MAX)
  if (count > static_cast<std::size_t>(IOV_MAX)) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
#endif

  return static_cast<int>(count);
}

[[nodiscard]]
IoAttempt TryReadv(int fd, std::span<const iovec> buffers) noexcept {
  if (buffers.empty()) {
    return IoAttempt::Completed(0);
  }

  auto count = CheckedIovCount(buffers.size());
  if (!count.has_value()) {
    return IoAttempt::Failed(count.error());
  }

  return RetryNonBlockingIo([fd, buffers, iov_count = *count]() noexcept {
    return ::readv(fd, buffers.data(), iov_count);
  });
}

[[nodiscard]]
IoAttempt TryWritev(int fd, std::span<const iovec> buffers) noexcept {
  if (buffers.empty()) {
    return IoAttempt::Completed(0);
  }

  auto count = CheckedIovCount(buffers.size());
  if (!count.has_value()) {
    return IoAttempt::Failed(count.error());
  }

  return RetryNonBlockingIo([fd, buffers, iov_count = *count]() noexcept {
    return ::writev(fd, buffers.data(), iov_count);
  });
}

// Called only after a reactor error event. SO_ERROR == 0 is inconsistent with
// that event, so use EIO as a stable error result rather than reporting errno 0.
[[nodiscard]]
base::Error ErrorFromSocketErrorEvent(int fd) noexcept {
  int err = 0;
  auto len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    return base::CurrentErrno();
  }
  if (err == 0) {
    err = EIO;
  }
  return base::MakeErrno(err);
}

}  // namespace

bool ReactorStream::ReadSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    result_.SetError(base::MakeErrno(ECANCELED));
    (void)completion_gate_.TryComplete();
    return false;
  }
  if (stream_->closed_ || stream_->socket_.fd() < 0) {
    result_.SetError(base::MakeErrno(EBADF));
    (void)completion_gate_.TryComplete();
    return false;
  }

  COROPACT_DCHECK(stream_->pending_read_ == nullptr,
                  "ReadSomeAwaiter: only one pending read is supported per stream");

  continuation_.Bind(continuation);
  IoAttempt attempt = TryRead(stream_->socket_.fd(), buffer_);
  if (!attempt.Pending()) {
    result_.SetResult(attempt.result);
    (void)completion_gate_.TryComplete();
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
        stream_->CompleteRead(std::unexpected(base::MakeErrno(ETIMEDOUT)));
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
  if (!completion_gate_.TryComplete()) {
    return;
  }
  if (timer_.Valid()) {
    stream_->loop_->Cancel(timer_);
    timer_ = {};
  }
  stream_ = nullptr;
  result_.SetResult(result);
  continuation_.Schedule();
}

void ReactorStream::ReadSomeAwaiter::OnReadyImpl() noexcept {
  IoAttempt attempt = TryRead(stream_->socket_.fd(), buffer_);
  if (attempt.Pending()) {
    return;
  }
  stream_->CompleteRead(std::move(attempt.result));
}

ReactorStream::BufferReadAwaiter::BufferReadAwaiter(ReactorStream& stream, net::Buffer& buffer,
                                                    std::size_t reserve,
                                                    std::chrono::milliseconds timeout) noexcept
    : stream_(&stream),
      buffer_(&buffer),
      reserve_(std::max<std::size_t>(reserve, 1)),
      timeout_(timeout) {}

bool ReactorStream::BufferReadAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    result_.SetError(base::MakeErrno(ECANCELED));
    (void)completion_gate_.TryComplete();
    return false;
  }
  if (stream_->closed_ || stream_->socket_.fd() < 0) {
    result_.SetError(base::MakeErrno(EBADF));
    (void)completion_gate_.TryComplete();
    return false;
  }

  COROPACT_DCHECK(stream_->pending_read_ == nullptr,
                  "BufferReadAwaiter: only one pending read is supported per stream");

  continuation_.Bind(continuation);

  if (!PrepareReservation()) {
    (void)completion_gate_.TryComplete();
    return false;
  }

  IoAttempt attempt = TryReadv(stream_->socket_.fd(), iovs_);
  if (!attempt.Pending()) {
    FinishAttempt(attempt.result);
    (void)completion_gate_.TryComplete();
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
        stream_->CompleteRead(std::unexpected(base::MakeErrno(ETIMEDOUT)));
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
  if (!completion_gate_.TryComplete()) {
    return;
  }
  if (timer_.Valid()) {
    stream_->loop_->Cancel(timer_);
    timer_ = {};
  }
  FinishAttempt(std::move(result));
  stream_ = nullptr;
  continuation_.Schedule();
}

void ReactorStream::BufferReadAwaiter::OnReadyImpl() noexcept {
  IoAttempt attempt = TryReadv(stream_->socket_.fd(), iovs_);
  if (attempt.Pending()) {
    return;
  }
  stream_->CompleteRead(std::move(attempt.result));
}

bool ReactorStream::BufferReadAwaiter::PrepareReservation() noexcept {
  try {
    iovs_ = buffer_->PrepareWrite(reserve_, 16);
  } catch (const std::bad_alloc&) {
    result_.SetError(base::MakeErrno(ENOMEM));
    return false;
  }

  if (iovs_.empty()) {
    result_.SetError(base::MakeErrno(ENOMEM));
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

ReactorStream::ReadIntoAwaiter::ReadIntoAwaiter(ReactorStream& stream, net::Buffer buffer,
                                                std::size_t reserve) noexcept
    : stream_(&stream), buffer_(std::move(buffer)), reserve_(std::max<std::size_t>(reserve, 1)) {}

bool ReactorStream::ReadIntoAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    result_.SetError(base::MakeErrno(ECANCELED));
    (void)completion_gate_.TryComplete();
    return false;
  }
  if (stream_->closed_ || stream_->socket_.fd() < 0) {
    result_.SetError(base::MakeErrno(EBADF));
    (void)completion_gate_.TryComplete();
    return false;
  }
  if (stream_->pending_read_ != nullptr) {
    result_.SetError(base::MakeErrno(EBUSY));
    (void)completion_gate_.TryComplete();
    return false;
  }

  continuation_.Bind(continuation);
  if (!PrepareReservation()) {
    (void)completion_gate_.TryComplete();
    return false;
  }

  IoAttempt attempt = TryReadv(stream_->socket_.fd(), iovs_);
  if (!attempt.Pending()) {
    FinishAttempt(attempt.result);
    (void)completion_gate_.TryComplete();
    return false;
  }

  stream_->pending_read_ = this;
  stream_->pending_read_kind_ = PendingReadKind::kReadInto;
  if (!stream_->channel_.IsReading()) {
    stream_->channel_.EnableReading();
  }
  return true;
}

net::ReadIntoOutcome ReactorStream::ReadIntoAwaiter::await_resume() noexcept {
  COROPACT_DCHECK(result_.HasResult(), "ReadIntoAwaiter: result is not ready");
  return {
      .result = result_.Take(),
      .buffer = std::move(buffer_),
  };
}

void ReactorStream::ReadIntoAwaiter::CompleteImpl(base::Result<std::size_t> result) noexcept {
  if (!completion_gate_.TryComplete()) {
    return;
  }
  FinishAttempt(result);
  stream_ = nullptr;
  continuation_.Schedule();
}

void ReactorStream::ReadIntoAwaiter::OnReadyImpl() noexcept {
  IoAttempt attempt = TryReadv(stream_->socket_.fd(), iovs_);
  if (attempt.Pending()) {
    return;
  }
  stream_->CompleteRead(attempt.result);
}

bool ReactorStream::ReadIntoAwaiter::PrepareReservation() noexcept {
  try {
    iovs_ = buffer_.PrepareWrite(reserve_, 16);
  } catch (const std::bad_alloc&) {
    buffer_.AbortWrite();
    result_.SetError(base::MakeErrno(ENOMEM));
    return false;
  }

  if (iovs_.empty()) {
    buffer_.AbortWrite();
    result_.SetError(base::MakeErrno(ENOMEM));
    return false;
  }
  reservation_active_ = true;
  return true;
}

void ReactorStream::ReadIntoAwaiter::FinishAttempt(base::Result<std::size_t> result) noexcept {
  COROPACT_CHECK(reservation_active_, "ReadIntoAwaiter completion without a buffer reservation");
  if (result.has_value()) {
    buffer_.CommitWrite(*result);
  } else {
    buffer_.AbortWrite();
  }
  reservation_active_ = false;
  result_.SetResult(result);
}

bool ReactorStream::WriteSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    result_.SetError(base::MakeErrno(ECANCELED));
    (void)completion_gate_.TryComplete();
    return false;
  }
  if (stream_->closed_ || stream_->socket_.fd() < 0) {
    result_.SetError(base::MakeErrno(EBADF));
    (void)completion_gate_.TryComplete();
    return false;
  }

  COROPACT_DCHECK(stream_->pending_write_ == nullptr,
                  "WriteSomeAwaiter: only one pending write is supported per stream");

  continuation_.Bind(continuation);
  IoAttempt attempt = TryWrite(stream_->socket_.fd(), buffer_);
  if (!attempt.Pending()) {
    result_.SetResult(attempt.result);
    (void)completion_gate_.TryComplete();
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
  if (!completion_gate_.TryComplete()) {
    return;
  }
  result_.SetResult(result);
  continuation_.Schedule();
}

void ReactorStream::WriteSomeAwaiter::OnReadyImpl() noexcept {
  IoAttempt attempt = TryWrite(stream_->socket_.fd(), buffer_);
  if (attempt.Pending()) {
    return;
  }
  stream_->CompleteWrite(std::move(attempt.result));
}

ReactorStream::WriteAllAwaiter ReactorStream::WriteAll(std::span<const std::byte> buffer) noexcept {
  return WriteAllAwaiter{*this, buffer};
}

bool ReactorStream::WriteAllAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    result_.SetError(base::MakeErrno(ECANCELED));
    (void)completion_gate_.TryComplete();
    return false;
  }
  if (stream_->closed_ || stream_->socket_.fd() < 0) {
    result_.SetError(base::MakeErrno(EBADF));
    (void)completion_gate_.TryComplete();
    return false;
  }

  COROPACT_DCHECK(stream_->pending_write_ == nullptr,
                  "WriteAllAwaiter: only one pending write is supported per stream");

  continuation_.Bind(continuation);
  while (!buffer_.empty()) {
    IoAttempt attempt = TryWrite(stream_->socket_.fd(), buffer_);
    if (attempt.Pending()) {
      stream_->pending_write_ = this;
      stream_->pending_write_kind_ = ReactorStream::PendingWriteKind::kWriteAll;
      if (!stream_->channel_.IsWriting()) {
        stream_->channel_.EnableWriting();
      }
      return true;
    }
    if (!attempt.result.has_value()) {
      result_.SetError(attempt.result.error());
      (void)completion_gate_.TryComplete();
      return false;
    }
    if (*attempt.result == 0) {
      result_.SetError(base::MakeErrno(EPIPE));
      (void)completion_gate_.TryComplete();
      return false;
    }
    buffer_ = buffer_.subspan(*attempt.result);
  }

  result_.SetSuccess(0);
  (void)completion_gate_.TryComplete();
  return false;
}

base::Result<void> ReactorStream::WriteAllAwaiter::await_resume() noexcept {
  auto result = result_.Take();
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  return base::Result<void>{};
}

void ReactorStream::WriteAllAwaiter::CompleteImpl(base::Result<std::size_t> result) noexcept {
  if (!completion_gate_.TryComplete()) {
    return;
  }
  result_.SetResult(result);
  continuation_.Schedule();
}

void ReactorStream::WriteAllAwaiter::OnReadyImpl() noexcept {
  while (!buffer_.empty()) {
    IoAttempt attempt = TryWrite(stream_->socket_.fd(), buffer_);
    if (attempt.Pending()) {
      return;
    }
    if (!attempt.result.has_value()) {
      stream_->CompleteWrite(attempt.result);
      return;
    }
    if (*attempt.result == 0) {
      stream_->CompleteWrite(std::unexpected(base::MakeErrno(EPIPE)));
      return;
    }
    buffer_ = buffer_.subspan(*attempt.result);
  }

  stream_->CompleteWrite(base::Result<std::size_t>{0});
}

ReactorStream::BufferWriteAwaiter::BufferWriteAwaiter(ReactorStream& stream,
                                                      net::Buffer& buffer) noexcept
    : stream_(&stream), buffer_(&buffer) {}

bool ReactorStream::BufferWriteAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    result_.SetError(base::MakeErrno(ECANCELED));
    (void)completion_gate_.TryComplete();
    return false;
  }
  if (stream_->closed_ || stream_->socket_.fd() < 0) {
    result_.SetError(base::MakeErrno(EBADF));
    (void)completion_gate_.TryComplete();
    return false;
  }

  COROPACT_DCHECK(stream_->pending_write_ == nullptr,
                  "BufferWriteAwaiter: only one pending write is supported per stream");

  continuation_.Bind(continuation);

  if (!PrepareReadable()) {
    (void)completion_gate_.TryComplete();
    return false;
  }

  IoAttempt attempt = TryWritev(stream_->socket_.fd(), iovs_);
  if (!attempt.Pending()) {
    FinishAttempt(attempt.result);
    (void)completion_gate_.TryComplete();
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
  if (!completion_gate_.TryComplete()) {
    return;
  }
  FinishAttempt(result);
  continuation_.Schedule();
}

void ReactorStream::BufferWriteAwaiter::OnReadyImpl() noexcept {
  IoAttempt attempt = TryWritev(stream_->socket_.fd(), iovs_);
  if (attempt.Pending()) {
    return;
  }
  stream_->CompleteWrite(attempt.result);
}

bool ReactorStream::BufferWriteAwaiter::PrepareReadable() noexcept {
  if (buffer_->Empty()) {
    result_.SetSuccess(0);
    return false;
  }

  try {
    iovs_ = buffer_->ReadableIov(16);
  } catch (const std::bad_alloc&) {
    result_.SetError(base::MakeErrno(ENOMEM));
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

ReactorStream::ReactorStream(EventLoop* loop, int fd, net::Endpoint peer,
                             ReactorStreamOptions options)
    : loop_(loop), socket_(fd), channel_(loop, fd), peer_(peer) {
  COROPACT_CHECK(loop_ != nullptr, "ReactorStream: loop must not be null");
  COROPACT_CHECK(loop_->IsInLoopThread(), "ReactorStream created from wrong EventLoop thread");
  [[maybe_unused]] auto nonblocking = net::set_non_blocking(fd, true);
  COROPACT_DCHECK(nonblocking.has_value(), "ReactorStream: failed to set non-blocking mode");

  // A stream keeps read interest across successful reads. Edge-triggered
  // delivery avoids the level-triggered disable/re-enable epoll_ctl pair on
  // every keep-alive request; every ReadSome still probes the socket first.
  channel_.SetEdgeTriggered(options.trigger_mode == TriggerMode::kEdgeTriggered);
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

ReactorStream::ReactorStream(ReactorStream&& other) noexcept
    : loop_(PrepareMove(other)),
      socket_(std::move(other.socket_)),
      channel_(std::move(other.channel_)),
      peer_(std::move(other.peer_)),
      closed_(other.closed_) {
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
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
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
  other.closed_ = true;
  return *this;
}

ReactorStream::~ReactorStream() {
  if (loop_ == nullptr) {
    return;
  }
  RequireOwnerLoop();
  COROPACT_DCHECK(pending_read_ == nullptr, "ReactorStream destroyed with a pending read");
  COROPACT_DCHECK(pending_write_ == nullptr, "ReactorStream destroyed with a pending write");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
}

ReactorStream::ReadSomeAwaiter ReactorStream::ReadSome(std::span<std::byte> buffer) noexcept {
  return ReadSomeAwaiter{*this, buffer};
}

ReactorStream::ReadIntoAwaiter ReactorStream::ReadInto(net::Buffer buffer,
                                                       std::size_t reserve) noexcept {
  return ReadIntoAwaiter{*this, std::move(buffer), reserve};
}

ReactorStream::BufferReadAwaiter ReactorStream::ReadSome(net::Buffer& buffer,
                                                         std::size_t reserve) noexcept {
  return BufferReadAwaiter{*this, buffer, reserve};
}

ReactorStream::ReadSomeAwaiter ReactorStream::ReadSomeFor(
    std::span<std::byte> buffer, std::chrono::milliseconds timeout) noexcept {
  return ReadSomeAwaiter{*this, buffer, timeout};
}

ReactorStream::BufferReadAwaiter ReactorStream::ReadSomeFor(net::Buffer& buffer,
                                                            std::chrono::milliseconds timeout,
                                                            std::size_t reserve) noexcept {
  return BufferReadAwaiter{*this, buffer, reserve, timeout};
}

ReactorStream::WriteSomeAwaiter ReactorStream::WriteSome(
    std::span<const std::byte> buffer) noexcept {
  return WriteSomeAwaiter{*this, buffer};
}

ReactorStream::BufferWriteAwaiter ReactorStream::WriteSome(net::Buffer& buffer) noexcept {
  return BufferWriteAwaiter{*this, buffer};
}

coro::Task<base::Result<void>> ReactorStream::Shutdown() {
  RequireOwnerLoop();
  if (closed_) {
    co_return std::unexpected(base::MakeErrno(EBADF));
  }
  socket_.ShutdownWrite();
  co_return base::Result<void>{};
}

coro::Task<base::Result<void>> ReactorStream::Close() {
  RequireOwnerLoop();
  CloseNow();
  co_return base::Result<void>{};
}

void ReactorStream::HandleRead() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::HandleRead called from wrong thread");
  if (pending_read_ == nullptr) {
    // Keep LT cheap for back-to-back reads, but disarm stale readiness when a
    // consumer did not submit the next read before the event loop polled
    // again. This prevents an unread remainder from spinning the loop.
    if (!channel_.IsEdgeTriggered() && channel_.IsReading()) {
      channel_.DisableReading();
    }
    return;
  }
  switch (pending_read_kind_) {
    case PendingReadKind::kReadSome:
      static_cast<ReadSomeAwaiter*>(pending_read_)->OnReady();
      return;
    case PendingReadKind::kReadInto:
      static_cast<ReadIntoAwaiter*>(pending_read_)->OnReady();
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
    case PendingWriteKind::kWriteAll:
      static_cast<WriteAllAwaiter*>(pending_write_)->OnReady();
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
  CompleteWrite(std::unexpected(base::MakeErrno(EPIPE)));
}

void ReactorStream::HandleError() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::HandleError called from wrong thread");
  base::Error error = ErrorFromSocketErrorEvent(socket_.fd());
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
  // Successful reads keep interest armed in both modes so a continuation can
  // immediately submit the next read without an epoll_ctl pair. LT disarms
  // lazily in HandleRead when readiness arrives without a pending operation.
  // Terminal results remove the interest in both modes.
  if (!result.has_value() || *result == 0) {
    if (channel_.IsReading()) {
      channel_.DisableReading();
    }
  }
  switch (kind) {
    case PendingReadKind::kReadSome:
      static_cast<ReadSomeAwaiter*>(awaiter)->Complete(result);
      return;
    case PendingReadKind::kReadInto:
      static_cast<ReadIntoAwaiter*>(awaiter)->Complete(result);
      return;
    case PendingReadKind::kBufferRead:
      static_cast<BufferReadAwaiter*>(awaiter)->Complete(result);
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
    case PendingWriteKind::kWriteAll:
      static_cast<WriteAllAwaiter*>(awaiter)->Complete(std::move(result));
      return;
    case PendingWriteKind::kBufferWrite:
      static_cast<BufferWriteAwaiter*>(awaiter)->Complete(std::move(result));
      return;
    case PendingWriteKind::kNone:
      COROPACT_CHECK(false, "ReactorStream::CompleteWrite missing operation kind");
      return;
  }
}

void ReactorStream::CloseNow() noexcept {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::CloseNow called from wrong thread");
  if (closed_) {
    return;
  }

  closed_ = true;
  if (pending_read_ != nullptr) {
    CompleteRead(std::unexpected(base::MakeErrno(ECANCELED)));
  }
  if (pending_write_ != nullptr) {
    CompleteWrite(std::unexpected(base::MakeErrno(ECANCELED)));
  }
  DetachChannel();
  socket_.Close();
}

void ReactorStream::DetachChannel() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::DetachChannel called from wrong thread");
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (channel_.IsRegistered()) {
    channel_.Remove();
  }
}

void ReactorStream::RequireOwnerLoop() const noexcept {
  COROPACT_CHECK(loop_ != nullptr, "ReactorStream operation has no owner EventLoop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "ReactorStream operation called from wrong EventLoop thread");
}

void ReactorStream::DispatchRead(void* context) noexcept {
  static_cast<ReactorStream*>(context)->HandleRead();
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
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
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
  LoopAccess::UnregisterShutdownParticipant(*other.loop_, other.shutdown_participant_);
  EventLoop* loop = std::exchange(other.loop_, nullptr);
  return loop;
}

void ReactorStream::DispatchLoopStop(void* context) noexcept {
  static_cast<ReactorStream*>(context)->CloseNow();
}

}  // namespace coropact::reactor
