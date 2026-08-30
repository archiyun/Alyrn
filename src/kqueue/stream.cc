// SPDX-License-Identifier: MIT
#include "alyrn/kqueue/stream.h"

#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <new>
#include <span>
#include <utility>

#include "alyrn/detail/check.h"
#include "alyrn/kqueue/detail/loop_access.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/result.h"

namespace alyrn::kqueue {

using detail::LoopAccess;

namespace {

constexpr std::size_t kReadIntoMaxIov = 16;

constexpr bool IsWouldBlock(int error) noexcept {
  return error == EAGAIN || error == EWOULDBLOCK;
}

enum class IoAttemptState : std::uint8_t {
  kCompleted,
  kWouldBlock,
};

struct IoAttempt {
  IoAttemptState state{IoAttemptState::kCompleted};
  Result<std::size_t> result{0};

  static IoAttempt Completed(std::size_t bytes) noexcept {
    return {
        .state = IoAttemptState::kCompleted,
        .result = bytes,
    };
  }

  static IoAttempt WouldBlock() noexcept {
    return {
        .state = IoAttemptState::kWouldBlock,
        .result = std::size_t{0},
    };
  }

  static IoAttempt Failed(Error error) noexcept {
    return {
        .state = IoAttemptState::kCompleted,
        .result = std::unexpected(error),
    };
  }
};

template <typename Operation>
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
    return IoAttempt::Failed(Errno(error));
  }
}

IoAttempt TryRead(int fd, std::span<std::byte> buffer) noexcept {
  return RetryNonBlockingIo(
      [fd, buffer]() noexcept { return ::read(fd, buffer.data(), buffer.size()); });
}

IoAttempt TryWrite(int fd, std::span<const std::byte> buffer) noexcept {
#if defined(MSG_NOSIGNAL)
  return RetryNonBlockingIo(
      [fd, buffer]() noexcept { return ::send(fd, buffer.data(), buffer.size(), MSG_NOSIGNAL); });
#else
  return RetryNonBlockingIo(
      [fd, buffer]() noexcept { return ::write(fd, buffer.data(), buffer.size()); });
#endif
}

Result<int> CheckedIovCount(std::size_t count) noexcept {
  if (count > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return std::unexpected(Errno(EINVAL));
  }

#if defined(IOV_MAX)
  if (count > static_cast<std::size_t>(IOV_MAX)) {
    return std::unexpected(Errno(EINVAL));
  }
#endif

  return static_cast<int>(count);
}

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

Error ErrorFromSocketErrorEvent(int fd) noexcept {
  int err = 0;
  auto len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    return CurrentErrno();
  }
  if (err == 0) {
    err = EIO;
  }
  return Errno(err);
}

void SuppressSigpipe(int fd) noexcept {
#if defined(SO_NOSIGPIPE)
  const int on = 1;
  (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#else
  (void)fd;
#endif
}

// One-shot delivery retires the filter before HandleEvent. A WouldBlock retry
// therefore has to re-arm explicitly or the wait stalls forever.
void RearmReading(detail::Channel& channel) noexcept {
  if (!channel.IsReading()) {
    channel.EnableReading();
  }
}

void RearmWriting(detail::Channel& channel) noexcept {
  if (!channel.IsWriting()) {
    channel.EnableWriting();
  }
}

}  // namespace

bool Stream::ReadAwaiterState::BeginRead(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    CompleteInline(std::unexpected(Errno(ECANCELED)));
    return false;
  }
  auto valid = stream_->lifecycle_.ValidateRead();
  if (!valid.has_value()) {
    CompleteInline(std::unexpected(valid.error()));
    return false;
  }
  if (stream_->socket_.fd() < 0) {
    CompleteInline(std::unexpected(Errno(EBADF)));
    return false;
  }

  if (stream_->lifecycle_.IsReadShutdown()) {
    CompleteInline(Result<std::size_t>{0});
    return false;
  }

  if (stream_->pending_read_ != nullptr) {
    CompleteInline(std::unexpected(Errno(EBUSY)));
    return false;
  }

  continuation_.Bind(continuation);
  return true;
}

void Stream::ReadAwaiterState::SuspendForRead(void* awaiter, PendingReadKind kind) noexcept {
  stream_->pending_read_ = awaiter;
  stream_->pending_read_kind_ = kind;
  RearmReading(stream_->channel_);
}

bool Stream::ReadAwaiterState::TryAuthorizeResult() noexcept {
  return lifecycle_.TryAuthorizeResult();
}

bool Stream::ReadAwaiterState::TryAuthorizeRelease() noexcept {
  return lifecycle_.TryAuthorizeRelease();
}

bool Stream::ReadAwaiterState::TryAuthorizeContinuation() noexcept {
  return lifecycle_.TryAuthorizeContinuation();
}

void Stream::ReadAwaiterState::CompleteInline(Result<std::size_t> result) noexcept {
  result_.SetResult(result);
  CompleteStoredInline();
}

void Stream::ReadAwaiterState::CompleteStoredInline() noexcept {
  ALYRN_CHECK(TryAuthorizeResult(), "Kqueue read result was authorized twice");
  ALYRN_CHECK(TryAuthorizeRelease(), "Kqueue read release was not authorized after its result");
}

void Stream::ReadAwaiterState::SetResult(Result<std::size_t> result) noexcept {
  result_.SetResult(result);
}

void Stream::ReadAwaiterState::ScheduleContinuation() noexcept { continuation_.Schedule(); }

Result<std::size_t> Stream::ReadAwaiterState::TakeResult() noexcept { return result_.Take(); }

bool Stream::ReadSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (!BeginRead(continuation)) {
    return false;
  }

  auto [state, result] = TryRead(stream_->socket_.fd(), buffer_);
  if (state != IoAttemptState::kWouldBlock) {
    CompleteInline(result);
    return false;
  }

  SuspendForRead(this, Stream::PendingReadKind::kReadSome);
  return true;
}

Result<std::size_t> Stream::ReadSomeAwaiter::await_resume() noexcept { return TakeResult(); }

bool Stream::ReadSomeAwaiter::CompleteResultImpl(Result<std::size_t> result) noexcept {
  if (!TryAuthorizeResult()) {
    return false;
  }
  stream_ = nullptr;
  SetResult(result);
  return true;
}

void Stream::ReadSomeAwaiter::OnReadyImpl() noexcept {
  auto [state, result] = TryRead(stream_->socket_.fd(), buffer_);
  if (state == IoAttemptState::kWouldBlock) {
    RearmReading(stream_->channel_);
    return;
  }
  stream_->CompleteRead(result);
}

Stream::ReadIntoAwaiter::ReadIntoAwaiter(Stream& stream, net::Buffer buffer,
                                         std::size_t reserve) noexcept
    : ReadAwaiterState(stream), buffer_(std::move(buffer)), reserve_(reserve) {}

bool Stream::ReadIntoAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (!BeginRead(continuation)) {
    return false;
  }

  if (!PrepareReservation()) {
    CompleteStoredInline();
    return false;
  }

  std::array<iovec, kReadIntoMaxIov> iovs;
  auto [state, result] = TryReadv(stream_->socket_.fd(), buffer_.ReservedWriteIov(iovs));
  if (state != IoAttemptState::kWouldBlock) {
    FinishAttempt(result);
    CompleteStoredInline();
    return false;
  }

  SuspendForRead(this, PendingReadKind::kReadInto);
  return true;
}

net::ReadIntoOutcome Stream::ReadIntoAwaiter::await_resume() noexcept {
  return {
      .result = TakeResult(),
      .buffer = std::move(buffer_),
  };
}

bool Stream::ReadIntoAwaiter::CompleteResultImpl(Result<std::size_t> result) noexcept {
  if (!TryAuthorizeResult()) {
    return false;
  }
  FinishAttempt(result);
  stream_ = nullptr;
  return true;
}

void Stream::ReadIntoAwaiter::OnReadyImpl() noexcept {
  std::array<iovec, kReadIntoMaxIov> iovs;
  auto [state, result] = TryReadv(stream_->socket_.fd(), buffer_.ReservedWriteIov(iovs));
  if (state == IoAttemptState::kWouldBlock) {
    RearmReading(stream_->channel_);
    return;
  }
  stream_->CompleteRead(result);
}

bool Stream::ReadIntoAwaiter::PrepareReservation() noexcept {
  try {
    std::array<iovec, kReadIntoMaxIov> iovs;
    if (buffer_.PrepareWrite(reserve_, iovs).empty()) {
      buffer_.AbortWrite();
      result_.SetError(Errno(ENOMEM));
      return false;
    }
  } catch (const std::bad_alloc&) {
    buffer_.AbortWrite();
    result_.SetError(Errno(ENOMEM));
    return false;
  }
  reservation_active_ = true;
  return true;
}

void Stream::ReadIntoAwaiter::FinishAttempt(Result<std::size_t> result) noexcept {
  ALYRN_CHECK(reservation_active_, "ReadIntoAwaiter completion without a buffer reservation");
  if (result.has_value()) {
    buffer_.CommitWrite(*result);
  } else {
    buffer_.AbortWrite();
  }
  reservation_active_ = false;
  result_.SetResult(result);
}

Stream::WriteAllAwaiter Stream::WriteAll(std::span<const std::byte> buffer) noexcept {
  return WriteAllAwaiter{*this, buffer};
}

bool Stream::WriteAllAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    CompleteInline(std::unexpected(Errno(ECANCELED)));
    return false;
  }
  auto valid = stream_->lifecycle_.ValidateWrite();
  if (!valid.has_value()) {
    CompleteInline(std::unexpected(valid.error()));
    return false;
  }
  if (stream_->socket_.fd() < 0) {
    CompleteInline(std::unexpected(Errno(EBADF)));
    return false;
  }

  if (stream_->pending_write_ != nullptr) {
    CompleteInline(std::unexpected(Errno(EBUSY)));
    return false;
  }

  continuation_.Bind(continuation);
  while (!buffer_.empty()) {
    auto [state, result] = TryWrite(stream_->socket_.fd(), buffer_);
    if (state == IoAttemptState::kWouldBlock) {
      stream_->pending_write_ = this;
      RearmWriting(stream_->channel_);
      return true;
    }
    if (!result.has_value()) {
      CompleteInline(result);
      return false;
    }
    if (*result == 0) {
      CompleteInline(std::unexpected(Errno(EPIPE)));
      return false;
    }
    buffer_ = buffer_.subspan(*result);
  }

  CompleteInline(std::size_t{0});
  return false;
}

Result<void> Stream::WriteAllAwaiter::await_resume() noexcept {
  auto result = result_.Take();
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  return Result<void>{};
}

void Stream::WriteAllAwaiter::CompleteInline(Result<std::size_t> result) noexcept {
  result_.SetResult(result);
  ALYRN_CHECK(lifecycle_.TryAuthorizeResult(), "Kqueue write result was authorized twice");
  ALYRN_CHECK(lifecycle_.TryAuthorizeRelease(),
                 "Kqueue write release was not authorized after its result");
}

bool Stream::WriteAllAwaiter::CompleteResultImpl(Result<std::size_t> result) noexcept {
  if (!lifecycle_.TryAuthorizeResult()) {
    return false;
  }
  result_.SetResult(result);
  return true;
}

bool Stream::WriteAllAwaiter::TryAuthorizeRelease() noexcept {
  return lifecycle_.TryAuthorizeRelease();
}

bool Stream::WriteAllAwaiter::TryAuthorizeContinuation() noexcept {
  return lifecycle_.TryAuthorizeContinuation();
}

void Stream::WriteAllAwaiter::ScheduleContinuation() noexcept { continuation_.Schedule(); }

void Stream::WriteAllAwaiter::OnReadyImpl() noexcept {
  while (!buffer_.empty()) {
    auto [state, result] = TryWrite(stream_->socket_.fd(), buffer_);
    if (state == IoAttemptState::kWouldBlock) {
      RearmWriting(stream_->channel_);
      return;
    }
    if (!result.has_value()) {
      stream_->CompleteWrite(result);
      return;
    }
    if (*result == 0) {
      stream_->CompleteWrite(std::unexpected(Errno(EPIPE)));
      return;
    }
    buffer_ = buffer_.subspan(*result);
  }

  stream_->CompleteWrite(Result<std::size_t>{0});
}

Stream::Stream(Loop* loop, int fd, net::Endpoint peer, StreamOptions options)
    : loop_(loop), socket_(fd), channel_(loop, fd), peer_(peer) {
  ALYRN_CHECK(loop_ != nullptr, "Stream: loop must not be null");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Stream created from wrong Loop thread");
  ALYRN_CHECK(options.trigger_mode == TriggerMode::kOneShot,
                 "Stream currently supports only TriggerMode::kOneShot");
  [[maybe_unused]] auto nonblocking = net::SetNonBlocking(fd, true);
  ALYRN_CHECK(nonblocking.has_value(), "Stream: failed to set non-blocking mode");
  SuppressSigpipe(fd);

  channel_.SetTriggerMode(TriggerMode::kOneShot);
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

Stream::Stream(Stream&& other) noexcept
    : loop_(PrepareMove(other)),
      socket_(std::move(other.socket_)),
      channel_(std::move(other.channel_)),
      peer_(other.peer_),
      lifecycle_(std::move(other.lifecycle_)) {
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

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

Stream::~Stream() {
  if (loop_ == nullptr) {
    return;
  }
  RequireOwnerLoop();
  ALYRN_CHECK(pending_read_ == nullptr, "Stream destroyed with a pending read");
  ALYRN_CHECK(pending_write_ == nullptr, "Stream destroyed with a pending write");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
}

int Stream::Release() noexcept {
  RequireOwnerLoop();
  ALYRN_CHECK(pending_read_ == nullptr, "Stream cannot release with a pending read");
  ALYRN_CHECK(pending_write_ == nullptr, "Stream cannot release with a pending write");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
  loop_ = nullptr;
  return socket_.Release();
}

Stream::ReadSomeAwaiter Stream::ReadSome(std::span<std::byte> buffer) noexcept {
  return ReadSomeAwaiter{*this, buffer};
}

Stream::ReadIntoAwaiter Stream::ReadInto(net::Buffer buffer, std::size_t reserve) noexcept {
  return ReadIntoAwaiter{*this, std::move(buffer), reserve};
}

coro::Task<Result<void>> Stream::Shutdown() noexcept { return CloseWrite(); }

coro::Task<Result<void>> Stream::CloseWrite() noexcept {
  RequireOwnerLoop();
  if (socket_.fd() < 0) {
    co_return std::unexpected(Errno(EBADF));
  }
  auto prepare = lifecycle_.PrepareShutdown(pending_write_ != nullptr);
  if (!prepare.has_value()) {
    co_return std::unexpected(prepare.error());
  }
  if (!*prepare) {
    co_return Result<void>{};
  }
  auto shutdown = socket_.ShutdownWrite();
  if (!shutdown.has_value()) {
    lifecycle_.AbortShutdownPreparation();
    co_return std::unexpected(shutdown.error());
  }
  lifecycle_.CommitShutdown();
  co_return Result<void>{};
}

coro::Task<Result<void>> Stream::CloseRead() noexcept {
  RequireOwnerLoop();
  if (socket_.fd() < 0) {
    co_return std::unexpected(Errno(EBADF));
  }
  auto prepare = lifecycle_.PrepareCloseRead(pending_read_ != nullptr);
  if (!prepare.has_value()) {
    co_return std::unexpected(prepare.error());
  }
  if (!*prepare) {
    co_return Result<void>{};
  }
  auto close_read = socket_.ShutdownRead();
  if (!close_read.has_value()) {
    lifecycle_.AbortCloseReadPreparation();
    co_return std::unexpected(close_read.error());
  }
  lifecycle_.CommitCloseRead();
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  co_return Result<void>{};
}

coro::Task<Result<void>> Stream::Close() noexcept {
  RequireOwnerLoop();
  CloseNow();
  co_return Result<void>{};
}

Result<net::Endpoint> Stream::LocalAddr() const noexcept {
  RequireOwnerLoop();

  if (socket_.fd() < 0) {
    return std::unexpected(Errno(EBADF));
  }

  return socket_.LocalEndpoint();
}

Result<void> Stream::SetNoDelay(bool enabled) const noexcept {
  RequireOwnerLoop();
  return net::SetNoDelay(socket_.fd(), enabled);
}

Result<void> Stream::SetKeepAlive(bool enabled) const noexcept {
  RequireOwnerLoop();
  return net::SetKeepAlive(socket_.fd(), enabled);
}

Result<void> Stream::SetKeepAlivePeriod(time::Duration period) const noexcept {
  RequireOwnerLoop();
  return net::SetKeepAlivePeriod(socket_.fd(), period);
}

Result<void> Stream::SetReadBuffer(std::size_t bytes) const noexcept {
  RequireOwnerLoop();
  return net::SetReadBuffer(socket_.fd(), bytes);
}

Result<void> Stream::SetWriteBuffer(std::size_t bytes) const noexcept {
  RequireOwnerLoop();
  return net::SetWriteBuffer(socket_.fd(), bytes);
}

void Stream::HandleRead() {
  ALYRN_DCHECK(loop_->IsInLoopThread(), "Stream::HandleRead called from wrong thread");
  if (pending_read_ == nullptr) {
    // One-shot delivery already retired the filter; nothing to disarm.
    return;
  }
  switch (pending_read_kind_) {
    case PendingReadKind::kReadSome:
      static_cast<ReadSomeAwaiter*>(pending_read_)->OnReady();
      return;
    case PendingReadKind::kReadInto:
      static_cast<ReadIntoAwaiter*>(pending_read_)->OnReady();
      return;
    case PendingReadKind::kNone:
      return;
  }
}

void Stream::HandleWrite() {
  ALYRN_DCHECK(loop_->IsInLoopThread(), "Stream::HandleWrite called from wrong thread");
  if (pending_write_ == nullptr) {
    return;
  }
  pending_write_->OnReady();
}

void Stream::HandleClose() {
  ALYRN_DCHECK(loop_->IsInLoopThread(), "Stream::HandleClose called from wrong thread");
  CompleteRead(Result<std::size_t>{0});
  CompleteWrite(std::unexpected(Errno(EPIPE)));
}

void Stream::HandleError() {
  ALYRN_DCHECK(loop_->IsInLoopThread(), "Stream::HandleError called from wrong thread");
  Error error = ErrorFromSocketErrorEvent(socket_.fd());
  CompleteRead(std::unexpected(error));
  CompleteWrite(std::unexpected(error));
}

void Stream::CompleteRead(Result<std::size_t> result) {
  ALYRN_DCHECK(loop_->IsInLoopThread(), "Stream::CompleteRead called from wrong thread");
  void* awaiter = pending_read_;
  const PendingReadKind kind = pending_read_kind_;
  if (awaiter == nullptr) {
    return;
  }

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
    case PendingReadKind::kNone:
      ALYRN_CHECK(false, "Stream::CompleteRead missing operation kind");
      return;
  }
  ALYRN_CHECK(result_authorized, "Stream::CompleteRead result was already authorized");
  ALYRN_CHECK(state != nullptr, "Stream::CompleteRead has no awaiter state");
  ALYRN_CHECK(state->TryAuthorizeRelease(),
                 "Stream::CompleteRead release was not authorized after its result");

  void* released = std::exchange(pending_read_, nullptr);
  const PendingReadKind released_kind = std::exchange(pending_read_kind_, PendingReadKind::kNone);
  ALYRN_CHECK(released == awaiter && released_kind == kind,
                 "Stream::CompleteRead pending slot changed during completion");

  // One-shot interest is already gone after delivery. A terminal path may
  // still see a re-armed filter from a WouldBlock retry; drop it so Close
  // does not leave a live registration behind.
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  ALYRN_CHECK(state->TryAuthorizeContinuation(),
                 "Stream::CompleteRead continuation was not authorized after release");
  state->ScheduleContinuation();
}

void Stream::CompleteWrite(Result<std::size_t> result) {
  ALYRN_DCHECK(loop_->IsInLoopThread(), "Stream::CompleteWrite called from wrong thread");
  WriteAllAwaiter* awaiter = pending_write_;
  if (awaiter == nullptr) {
    return;
  }
  ALYRN_CHECK(awaiter->CompleteResult(std::move(result)),
                 "Stream::CompleteWrite result was already authorized");
  ALYRN_CHECK(awaiter->TryAuthorizeRelease(),
                 "Stream::CompleteWrite release was not authorized after its result");

  WriteAllAwaiter* released = std::exchange(pending_write_, nullptr);
  ALYRN_CHECK(released == awaiter,
                 "Stream::CompleteWrite pending slot changed during completion");
  if (channel_.IsWriting()) {
    channel_.DisableWriting();
  }
  ALYRN_CHECK(awaiter->TryAuthorizeContinuation(),
                 "Stream::CompleteWrite continuation was not authorized after release");
  awaiter->ScheduleContinuation();
}

void Stream::CloseNow() noexcept {
  ALYRN_DCHECK(loop_->IsInLoopThread(), "Stream::CloseNow called from wrong thread");
  auto close_prepared = lifecycle_.PrepareClose();
  if (!close_prepared.has_value() || !*close_prepared) {
    return;
  }

  if (pending_read_ != nullptr) {
    CompleteRead(std::unexpected(Errno(ECANCELED)));
  }
  if (pending_write_ != nullptr) {
    CompleteWrite(std::unexpected(Errno(ECANCELED)));
  }
  DetachChannel();
  socket_.Close();
  lifecycle_.MarkClosed();
}

void Stream::DetachChannel() {
  ALYRN_DCHECK(loop_->IsInLoopThread(), "Stream::DetachChannel called from wrong thread");
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (channel_.IsRegistered()) {
    channel_.Remove();
  }
}

void Stream::RequireOwnerLoop() const noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Stream operation has no owner Loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Stream operation called from wrong Loop thread");
}

void Stream::DispatchRead(void* context) noexcept { static_cast<Stream*>(context)->HandleRead(); }

void Stream::DispatchWrite(void* context) noexcept { static_cast<Stream*>(context)->HandleWrite(); }

void Stream::DispatchClose(void* context) noexcept { static_cast<Stream*>(context)->HandleClose(); }

void Stream::DispatchError(void* context) noexcept { static_cast<Stream*>(context)->HandleError(); }

void Stream::BindChannelCallbacks() noexcept {
  try {
    channel_.SetReadCallback(&Stream::DispatchRead, this);
    channel_.SetWriteCallback(&Stream::DispatchWrite, this);
    channel_.SetCloseCallback(&Stream::DispatchClose, this);
    channel_.SetErrorCallback(&Stream::DispatchError, this);
  } catch (...) {
    ALYRN_CHECK(false, "Stream: failed to bind channel callbacks");
  }
}

void Stream::ResetForMove() noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Stream move destination is not initialized");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Stream move called from wrong Loop thread");
  ALYRN_CHECK(pending_read_ == nullptr, "Stream move destination has a pending read");
  ALYRN_CHECK(pending_write_ == nullptr, "Stream move destination has a pending write");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
  socket_.Close();
}

Loop* Stream::PrepareMove(Stream& other) noexcept {
  ALYRN_CHECK(other.loop_ != nullptr, "Stream move source is not initialized");
  ALYRN_CHECK(other.loop_->IsInLoopThread(), "Stream move called from wrong Loop thread");
  ALYRN_CHECK(other.pending_read_ == nullptr,
                 "Stream cannot move with a pending read operation");
  ALYRN_CHECK(other.pending_write_ == nullptr,
                 "Stream cannot move with a pending write operation");

  other.DetachChannel();
  LoopAccess::UnregisterShutdownParticipant(*other.loop_, other.shutdown_participant_);
  Loop* loop = std::exchange(other.loop_, nullptr);
  return loop;
}

void Stream::DispatchLoopStop(void* context) noexcept { static_cast<Stream*>(context)->CloseNow(); }

}  // namespace alyrn::kqueue
