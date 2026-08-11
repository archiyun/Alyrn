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
#include "coropact/net/socket.h"
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
      [fd, buffer]() noexcept { return ::send(fd, buffer.data(), buffer.size(), MSG_NOSIGNAL); });
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

// --- ReadAwaiterState ---
bool ReactorStream::ReadAwaiterState::BeginRead(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    CompleteInline(std::unexpected(base::MakeErrno(ECANCELED)));
    return false;
  }
  auto valid = stream_->lifecycle_.ValidateRead();
  if (!valid.has_value()) {
    CompleteInline(std::unexpected(valid.error()));
    return false;
  }
  if (stream_->socket_.fd() < 0) {
    CompleteInline(std::unexpected(base::MakeErrno(EBADF)));
    return false;
  }

  if (stream_->pending_read_ != nullptr) {
    CompleteInline(std::unexpected(base::MakeErrno(EBUSY)));
    return false;
  }

  continuation_.Bind(continuation);
  return true;
}

void ReactorStream::ReadAwaiterState::SuspendForRead(void* awaiter, PendingReadKind kind) noexcept {
  stream_->pending_read_ = awaiter;
  stream_->pending_read_kind_ = kind;
  if (!stream_->channel_.IsReading()) {
    stream_->channel_.EnableReading();
  }
}

void ReactorStream::ReadAwaiterState::ArmReadTimeout(std::chrono::milliseconds timeout,
                                                     void* awaiter, time::TimerId& timer) noexcept {
  if (timeout.count() <= 0) {
    return;
  }

  const auto seconds =
      std::chrono::duration<double>(std::max(timeout, std::chrono::milliseconds{1})).count();
  timer = stream_->loop_->RunAfter(seconds, [this, awaiter] {
    if (stream_ != nullptr && stream_->pending_read_ == awaiter) {
      stream_->CompleteRead(std::unexpected(base::MakeErrno(ETIMEDOUT)));
    }
  });
}

void ReactorStream::ReadAwaiterState::CancelReadTimeout(time::TimerId& timer) noexcept {
  if (!timer.Valid()) {
    return;
  }
  stream_->loop_->Cancel(timer);
  timer = {};
}

bool ReactorStream::ReadAwaiterState::TryAuthorizeResult() noexcept {
  return lifecycle_.TryAuthorizeResult();
}

bool ReactorStream::ReadAwaiterState::TryAuthorizeRelease() noexcept {
  return lifecycle_.TryAuthorizeRelease();
}

bool ReactorStream::ReadAwaiterState::TryAuthorizeContinuation() noexcept {
  return lifecycle_.TryAuthorizeContinuation();
}

void ReactorStream::ReadAwaiterState::CompleteInline(base::Result<std::size_t> result) noexcept {
  result_.SetResult(result);
  CompleteStoredInline();
}

void ReactorStream::ReadAwaiterState::CompleteStoredInline() noexcept {
  COROPACT_CHECK(TryAuthorizeResult(), "Reactor read result was authorized twice");
  COROPACT_CHECK(TryAuthorizeRelease(), "Reactor read release was not authorized after its result");
}

void ReactorStream::ReadAwaiterState::SetResult(base::Result<std::size_t> result) noexcept {
  result_.SetResult(result);
}

void ReactorStream::ReadAwaiterState::ScheduleContinuation() noexcept { continuation_.Schedule(); }

base::Result<std::size_t> ReactorStream::ReadAwaiterState::TakeResult() noexcept {
  return result_.Take();
}

// --- ReadSomeAwaiter ---
bool ReactorStream::ReadSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (!BeginRead(continuation)) {
    return false;
  }

  auto [state, result] = TryRead(stream_->socket_.fd(), buffer_);
  if (state != IoAttemptState::kWouldBlock) {
    CompleteInline(result);
    return false;
  }

  SuspendForRead(this, ReactorStream::PendingReadKind::kReadSome);
  ArmReadTimeout(timeout_, this, timer_);
  return true;
}

base::Result<std::size_t> ReactorStream::ReadSomeAwaiter::await_resume() noexcept {
  return TakeResult();
}

bool ReactorStream::ReadSomeAwaiter::CompleteResultImpl(base::Result<std::size_t> result) noexcept {
  if (!TryAuthorizeResult()) {
    return false;
  }
  CancelReadTimeout(timer_);
  stream_ = nullptr;
  SetResult(result);
  return true;
}

void ReactorStream::ReadSomeAwaiter::OnReadyImpl() noexcept {
  auto [state, result] = TryRead(stream_->socket_.fd(), buffer_);
  if (state == IoAttemptState::kWouldBlock) {
    return;
  }
  stream_->CompleteRead(result);
}

// --- BufferReadAwaiter ---
ReactorStream::BufferReadAwaiter::BufferReadAwaiter(ReactorStream& stream, net::Buffer& buffer,
                                                    std::size_t reserve,
                                                    std::chrono::milliseconds timeout) noexcept
    : ReadAwaiterState(stream), buffer_(&buffer), reserve_(reserve), timeout_(timeout) {}

bool ReactorStream::BufferReadAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  if (!BeginRead(continuation)) {
    return false;
  }

  if (!PrepareReservation()) {
    CompleteStoredInline();
    return false;
  }

  auto [state, result] = TryReadv(stream_->socket_.fd(), iovs_);
  if (state != IoAttemptState::kWouldBlock) {
    FinishAttempt(result);
    CompleteStoredInline();
    return false;
  }

  SuspendForRead(this, ReactorStream::PendingReadKind::kBufferRead);
  ArmReadTimeout(timeout_, this, timer_);
  return true;
}

base::Result<std::size_t> ReactorStream::BufferReadAwaiter::await_resume() noexcept {
  return TakeResult();
}

bool ReactorStream::BufferReadAwaiter::CompleteResultImpl(
    base::Result<std::size_t> result) noexcept {
  if (!TryAuthorizeResult()) {
    return false;
  }
  CancelReadTimeout(timer_);
  FinishAttempt(result);
  stream_ = nullptr;
  return true;
}

void ReactorStream::BufferReadAwaiter::OnReadyImpl() noexcept {
  auto [state, result] = TryReadv(stream_->socket_.fd(), iovs_);
  if (state == IoAttemptState::kWouldBlock) {
    return;
  }
  stream_->CompleteRead(result);
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

// --- ReadIntoAwaiter ---
ReactorStream::ReadIntoAwaiter::ReadIntoAwaiter(ReactorStream& stream, net::Buffer buffer,
                                                std::size_t reserve) noexcept
    : ReadAwaiterState(stream), buffer_(std::move(buffer)), reserve_(reserve) {}

bool ReactorStream::ReadIntoAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (!BeginRead(continuation)) {
    return false;
  }

  if (!PrepareReservation()) {
    CompleteStoredInline();
    return false;
  }

  auto [state, result] = TryReadv(stream_->socket_.fd(), iovs_);
  if (state != IoAttemptState::kWouldBlock) {
    FinishAttempt(result);
    CompleteStoredInline();
    return false;
  }

  SuspendForRead(this, PendingReadKind::kReadInto);
  return true;
}

net::ReadIntoOutcome ReactorStream::ReadIntoAwaiter::await_resume() noexcept {
  return {
      .result = TakeResult(),
      .buffer = std::move(buffer_),
  };
}

bool ReactorStream::ReadIntoAwaiter::CompleteResultImpl(base::Result<std::size_t> result) noexcept {
  if (!TryAuthorizeResult()) {
    return false;
  }
  FinishAttempt(result);
  stream_ = nullptr;
  return true;
}

void ReactorStream::ReadIntoAwaiter::OnReadyImpl() noexcept {
  auto [state, result] = TryReadv(stream_->socket_.fd(), iovs_);
  if (state == IoAttemptState::kWouldBlock) {
    return;
  }
  stream_->CompleteRead(result);
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

// --- WriteAllAwaiter ---
ReactorStream::WriteAllAwaiter ReactorStream::WriteAll(std::span<const std::byte> buffer) noexcept {
  return WriteAllAwaiter{*this, buffer};
}

bool ReactorStream::WriteAllAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    CompleteInline(std::unexpected(base::MakeErrno(ECANCELED)));
    return false;
  }
  auto valid = stream_->lifecycle_.ValidateWrite();
  if (!valid.has_value()) {
    CompleteInline(std::unexpected(valid.error()));
    return false;
  }
  if (stream_->socket_.fd() < 0) {
    CompleteInline(std::unexpected(base::MakeErrno(EBADF)));
    return false;
  }

  if (stream_->pending_write_ != nullptr) {
    CompleteInline(std::unexpected(base::MakeErrno(EBUSY)));
    return false;
  }

  continuation_.Bind(continuation);
  while (!buffer_.empty()) {
    auto [state, result] = TryWrite(stream_->socket_.fd(), buffer_);
    if (state == IoAttemptState::kWouldBlock) {
      stream_->pending_write_ = this;
      if (!stream_->channel_.IsWriting()) {
        stream_->channel_.EnableWriting();
      }
      return true;
    }
    if (!result.has_value()) {
      CompleteInline(result);
      return false;
    }
    if (*result == 0) {
      CompleteInline(std::unexpected(base::MakeErrno(EPIPE)));
      return false;
    }
    buffer_ = buffer_.subspan(*result);
  }

  CompleteInline(std::size_t{0});
  return false;
}

base::Result<void> ReactorStream::WriteAllAwaiter::await_resume() noexcept {
  auto result = result_.Take();
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  return base::Result<void>{};
}

void ReactorStream::WriteAllAwaiter::CompleteInline(base::Result<std::size_t> result) noexcept {
  result_.SetResult(result);
  COROPACT_CHECK(lifecycle_.TryAuthorizeResult(), "Reactor write result was authorized twice");
  COROPACT_CHECK(lifecycle_.TryAuthorizeRelease(),
                 "Reactor write release was not authorized after its result");
}

bool ReactorStream::WriteAllAwaiter::CompleteResultImpl(base::Result<std::size_t> result) noexcept {
  if (!lifecycle_.TryAuthorizeResult()) {
    return false;
  }
  result_.SetResult(result);
  return true;
}

bool ReactorStream::WriteAllAwaiter::TryAuthorizeRelease() noexcept {
  return lifecycle_.TryAuthorizeRelease();
}

bool ReactorStream::WriteAllAwaiter::TryAuthorizeContinuation() noexcept {
  return lifecycle_.TryAuthorizeContinuation();
}

void ReactorStream::WriteAllAwaiter::ScheduleContinuation() noexcept { continuation_.Schedule(); }

void ReactorStream::WriteAllAwaiter::OnReadyImpl() noexcept {
  while (!buffer_.empty()) {
    auto [state, result] = TryWrite(stream_->socket_.fd(), buffer_);
    if (state == IoAttemptState::kWouldBlock) {
      return;
    }
    if (!result.has_value()) {
      stream_->CompleteWrite(result);
      return;
    }
    if (*result == 0) {
      stream_->CompleteWrite(std::unexpected(base::MakeErrno(EPIPE)));
      return;
    }
    buffer_ = buffer_.subspan(*result);
  }

  stream_->CompleteWrite(base::Result<std::size_t>{0});
}

ReactorStream::ReactorStream(EventLoop* loop, int fd, net::Endpoint peer,
                             ReactorStreamOptions options)
    : loop_(loop), socket_(fd), channel_(loop, fd), peer_(peer) {
  COROPACT_CHECK(loop_ != nullptr, "ReactorStream: loop must not be null");
  COROPACT_CHECK(loop_->IsInLoopThread(), "ReactorStream created from wrong EventLoop thread");
  [[maybe_unused]] auto nonblocking = net::SetNonBlocking(fd, true);
  COROPACT_CHECK(nonblocking.has_value(), "ReactorStream: failed to set non-blocking mode");

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
      peer_(other.peer_),
      lifecycle_(std::move(other.lifecycle_)) {
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
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
  peer_ = other.peer_;
  pending_read_ = nullptr;
  pending_write_ = nullptr;
  lifecycle_ = std::move(other.lifecycle_);
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
  return *this;
}

ReactorStream::~ReactorStream() {
  if (loop_ == nullptr) {
    return;
  }
  RequireOwnerLoop();
  COROPACT_CHECK(pending_read_ == nullptr, "ReactorStream destroyed with a pending read");
  COROPACT_CHECK(pending_write_ == nullptr, "ReactorStream destroyed with a pending write");
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

coro::Task<base::Result<void>> ReactorStream::Shutdown() {
  RequireOwnerLoop();
  if (socket_.fd() < 0) {
    co_return std::unexpected(base::MakeErrno(EBADF));
  }
  auto prepare = lifecycle_.PrepareShutdown(pending_write_ != nullptr);
  if (!prepare.has_value()) {
    co_return std::unexpected(prepare.error());
  }
  if (!*prepare) {
    co_return base::Result<void>{};
  }
  auto shutdown = socket_.ShutdownWrite();
  if (!shutdown.has_value()) {
    lifecycle_.AbortShutdownPreparation();
    co_return std::unexpected(shutdown.error());
  }
  lifecycle_.CommitShutdown();
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
  pending_write_->OnReady();
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
  void* awaiter = pending_read_;
  const PendingReadKind kind = pending_read_kind_;
  if (awaiter == nullptr) {
    return;
  }

  const bool terminal_result = !result.has_value() || *result == 0;
  ReadAwaiterState* state = nullptr;
  bool result_authorized = false;
  switch (kind) {
    case PendingReadKind::kReadSome:
      state = static_cast<ReadSomeAwaiter*>(awaiter);
      result_authorized = static_cast<ReadSomeAwaiter*>(awaiter)->CompleteResult(std::move(result));
      break;
    case PendingReadKind::kReadInto:
      state = static_cast<ReadIntoAwaiter*>(awaiter);
      result_authorized = static_cast<ReadIntoAwaiter*>(awaiter)->CompleteResult(std::move(result));
      break;
    case PendingReadKind::kBufferRead:
      state = static_cast<BufferReadAwaiter*>(awaiter);
      result_authorized =
          static_cast<BufferReadAwaiter*>(awaiter)->CompleteResult(std::move(result));
      break;
    case PendingReadKind::kNone:
      COROPACT_CHECK(false, "ReactorStream::CompleteRead missing operation kind");
      return;
  }
  COROPACT_CHECK(result_authorized, "ReactorStream::CompleteRead result was already authorized");
  COROPACT_CHECK(state != nullptr, "ReactorStream::CompleteRead has no awaiter state");
  COROPACT_CHECK(state->TryAuthorizeRelease(),
                 "ReactorStream::CompleteRead release was not authorized after its result");

  // The stream slot is an operation resource. Release it only after the
  // result is fixed, but before continuation authorization: resumed code may
  // immediately submit the next read.
  void* released = std::exchange(pending_read_, nullptr);
  const PendingReadKind released_kind = std::exchange(pending_read_kind_, PendingReadKind::kNone);
  COROPACT_CHECK(released == awaiter && released_kind == kind,
                 "ReactorStream::CompleteRead pending slot changed during completion");

  // Successful reads keep interest armed in both modes so a continuation can
  // immediately submit the next read without an epoll_ctl pair. LT disarms
  // lazily in HandleRead when readiness arrives without a pending operation.
  // Terminal results remove the interest in both modes.
  if (terminal_result) {
    if (channel_.IsReading()) {
      channel_.DisableReading();
    }
  }
  COROPACT_CHECK(state->TryAuthorizeContinuation(),
                 "ReactorStream::CompleteRead continuation was not authorized after release");
  state->ScheduleContinuation();
}

void ReactorStream::CompleteWrite(base::Result<std::size_t> result) {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::CompleteWrite called from wrong thread");
  WriteAllAwaiter* awaiter = pending_write_;
  if (awaiter == nullptr) {
    return;
  }
  COROPACT_CHECK(awaiter->CompleteResult(std::move(result)),
                 "ReactorStream::CompleteWrite result was already authorized");
  COROPACT_CHECK(awaiter->TryAuthorizeRelease(),
                 "ReactorStream::CompleteWrite release was not authorized after its result");

  WriteAllAwaiter* released = std::exchange(pending_write_, nullptr);
  COROPACT_CHECK(released == awaiter,
                 "ReactorStream::CompleteWrite pending slot changed during completion");
  if (channel_.IsWriting()) {
    channel_.DisableWriting();
  }
  COROPACT_CHECK(awaiter->TryAuthorizeContinuation(),
                 "ReactorStream::CompleteWrite continuation was not authorized after release");
  awaiter->ScheduleContinuation();
}

void ReactorStream::CloseNow() noexcept {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorStream::CloseNow called from wrong thread");
  auto close_prepared = lifecycle_.PrepareClose();
  if (!close_prepared.has_value() || !*close_prepared) {
    return;
  }

  if (pending_read_ != nullptr) {
    CompleteRead(std::unexpected(base::MakeErrno(ECANCELED)));
  }
  if (pending_write_ != nullptr) {
    CompleteWrite(std::unexpected(base::MakeErrno(ECANCELED)));
  }
  DetachChannel();
  socket_.Close();
  lifecycle_.MarkClosed();
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
