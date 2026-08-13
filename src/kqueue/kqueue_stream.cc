// SPDX-License-Identifier: MIT
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
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

#include "coropact/base/check.h"
#include "coropact/result.h"
#include "coropact/kqueue/detail/loop_access.h"
#include "coropact/kqueue/stream.h"
#include "coropact/net/socket.h"

namespace coropact::kqueue {

using detail::LoopAccess;

namespace {

constexpr std::size_t kReadIntoMaxIov = 16;

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
  Result<std::size_t> result{0};

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
  static IoAttempt Failed(Error error) noexcept {
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
    return IoAttempt::Failed(Errno(error));
  }
}

[[nodiscard]]
IoAttempt TryRead(int fd, std::span<std::byte> buffer) noexcept {
  return RetryNonBlockingIo(
      [fd, buffer]() noexcept { return ::read(fd, buffer.data(), buffer.size()); });
}

[[nodiscard]]
IoAttempt TryWrite(int fd, std::span<const std::byte> buffer) noexcept {
#if defined(MSG_NOSIGNAL)
  return RetryNonBlockingIo(
      [fd, buffer]() noexcept { return ::send(fd, buffer.data(), buffer.size(), MSG_NOSIGNAL); });
#else
  return RetryNonBlockingIo(
      [fd, buffer]() noexcept { return ::write(fd, buffer.data(), buffer.size()); });
#endif
}

[[nodiscard]]
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

bool KqueueStream::ReadAwaiterState::BeginRead(std::coroutine_handle<> continuation) noexcept {
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

  if (stream_->pending_read_ != nullptr) {
    CompleteInline(std::unexpected(Errno(EBUSY)));
    return false;
  }

  continuation_.Bind(continuation);
  return true;
}

void KqueueStream::ReadAwaiterState::SuspendForRead(void* awaiter, PendingReadKind kind) noexcept {
  stream_->pending_read_ = awaiter;
  stream_->pending_read_kind_ = kind;
  RearmReading(stream_->channel_);
}

void KqueueStream::ReadAwaiterState::ArmReadTimeout(time::Duration timeout, void* awaiter,
                                                    time::TimerId& timer) noexcept {
  if (timeout <= time::Duration::zero()) {
    return;
  }

  timer = stream_->loop_->RunAfter(std::max(timeout, time::Microseconds(100)), [this, awaiter] {
    if (stream_ != nullptr && stream_->pending_read_ == awaiter) {
      stream_->CompleteRead(std::unexpected(Errno(ETIMEDOUT)));
    }
  });
}

void KqueueStream::ReadAwaiterState::CancelReadTimeout(time::TimerId& timer) noexcept {
  if (!timer.Valid()) {
    return;
  }
  stream_->loop_->Cancel(timer);
  timer = {};
}

bool KqueueStream::ReadAwaiterState::TryAuthorizeResult() noexcept {
  return lifecycle_.TryAuthorizeResult();
}

bool KqueueStream::ReadAwaiterState::TryAuthorizeRelease() noexcept {
  return lifecycle_.TryAuthorizeRelease();
}

bool KqueueStream::ReadAwaiterState::TryAuthorizeContinuation() noexcept {
  return lifecycle_.TryAuthorizeContinuation();
}

void KqueueStream::ReadAwaiterState::CompleteInline(Result<std::size_t> result) noexcept {
  result_.SetResult(result);
  CompleteStoredInline();
}

void KqueueStream::ReadAwaiterState::CompleteStoredInline() noexcept {
  COROPACT_CHECK(TryAuthorizeResult(), "Kqueue read result was authorized twice");
  COROPACT_CHECK(TryAuthorizeRelease(), "Kqueue read release was not authorized after its result");
}

void KqueueStream::ReadAwaiterState::SetResult(Result<std::size_t> result) noexcept {
  result_.SetResult(result);
}

void KqueueStream::ReadAwaiterState::ScheduleContinuation() noexcept { continuation_.Schedule(); }

Result<std::size_t> KqueueStream::ReadAwaiterState::TakeResult() noexcept {
  return result_.Take();
}

bool KqueueStream::ReadSomeAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (!BeginRead(continuation)) {
    return false;
  }

  auto [state, result] = TryRead(stream_->socket_.fd(), buffer_);
  if (state != IoAttemptState::kWouldBlock) {
    CompleteInline(result);
    return false;
  }

  SuspendForRead(this, KqueueStream::PendingReadKind::kReadSome);
  ArmReadTimeout(timeout_, this, timer_);
  return true;
}

Result<std::size_t> KqueueStream::ReadSomeAwaiter::await_resume() noexcept {
  return TakeResult();
}

bool KqueueStream::ReadSomeAwaiter::CompleteResultImpl(Result<std::size_t> result) noexcept {
  if (!TryAuthorizeResult()) {
    return false;
  }
  CancelReadTimeout(timer_);
  stream_ = nullptr;
  SetResult(result);
  return true;
}

void KqueueStream::ReadSomeAwaiter::OnReadyImpl() noexcept {
  auto [state, result] = TryRead(stream_->socket_.fd(), buffer_);
  if (state == IoAttemptState::kWouldBlock) {
    RearmReading(stream_->channel_);
    return;
  }
  stream_->CompleteRead(result);
}

KqueueStream::ReadIntoAwaiter::ReadIntoAwaiter(KqueueStream& stream, net::Buffer buffer,
                                               std::size_t reserve) noexcept
    : ReadAwaiterState(stream), buffer_(std::move(buffer)), reserve_(reserve) {}

bool KqueueStream::ReadIntoAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
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

net::ReadIntoOutcome KqueueStream::ReadIntoAwaiter::await_resume() noexcept {
  return {
      .result = TakeResult(),
      .buffer = std::move(buffer_),
  };
}

bool KqueueStream::ReadIntoAwaiter::CompleteResultImpl(Result<std::size_t> result) noexcept {
  if (!TryAuthorizeResult()) {
    return false;
  }
  FinishAttempt(result);
  stream_ = nullptr;
  return true;
}

void KqueueStream::ReadIntoAwaiter::OnReadyImpl() noexcept {
  std::array<iovec, kReadIntoMaxIov> iovs;
  auto [state, result] = TryReadv(stream_->socket_.fd(), buffer_.ReservedWriteIov(iovs));
  if (state == IoAttemptState::kWouldBlock) {
    RearmReading(stream_->channel_);
    return;
  }
  stream_->CompleteRead(result);
}

bool KqueueStream::ReadIntoAwaiter::PrepareReservation() noexcept {
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

void KqueueStream::ReadIntoAwaiter::FinishAttempt(Result<std::size_t> result) noexcept {
  COROPACT_CHECK(reservation_active_, "ReadIntoAwaiter completion without a buffer reservation");
  if (result.has_value()) {
    buffer_.CommitWrite(*result);
  } else {
    buffer_.AbortWrite();
  }
  reservation_active_ = false;
  result_.SetResult(result);
}

KqueueStream::WriteAllAwaiter KqueueStream::WriteAll(std::span<const std::byte> buffer) noexcept {
  return WriteAllAwaiter{*this, buffer};
}

bool KqueueStream::WriteAllAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
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

Result<void> KqueueStream::WriteAllAwaiter::await_resume() noexcept {
  auto result = result_.Take();
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  return Result<void>{};
}

void KqueueStream::WriteAllAwaiter::CompleteInline(Result<std::size_t> result) noexcept {
  result_.SetResult(result);
  COROPACT_CHECK(lifecycle_.TryAuthorizeResult(), "Kqueue write result was authorized twice");
  COROPACT_CHECK(lifecycle_.TryAuthorizeRelease(),
                 "Kqueue write release was not authorized after its result");
}

bool KqueueStream::WriteAllAwaiter::CompleteResultImpl(Result<std::size_t> result) noexcept {
  if (!lifecycle_.TryAuthorizeResult()) {
    return false;
  }
  result_.SetResult(result);
  return true;
}

bool KqueueStream::WriteAllAwaiter::TryAuthorizeRelease() noexcept {
  return lifecycle_.TryAuthorizeRelease();
}

bool KqueueStream::WriteAllAwaiter::TryAuthorizeContinuation() noexcept {
  return lifecycle_.TryAuthorizeContinuation();
}

void KqueueStream::WriteAllAwaiter::ScheduleContinuation() noexcept { continuation_.Schedule(); }

void KqueueStream::WriteAllAwaiter::OnReadyImpl() noexcept {
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

KqueueStream::KqueueStream(KqueueLoop* loop, int fd, net::Endpoint peer,
                           KqueueStreamOptions options)
    : loop_(loop), socket_(fd), channel_(loop, fd), peer_(peer) {
  COROPACT_CHECK(loop_ != nullptr, "KqueueStream: loop must not be null");
  COROPACT_CHECK(loop_->IsInLoopThread(), "KqueueStream created from wrong KqueueLoop thread");
  COROPACT_CHECK(options.trigger_mode == TriggerMode::kOneShot,
                 "KqueueStream currently supports only TriggerMode::kOneShot");
  [[maybe_unused]] auto nonblocking = net::SetNonBlocking(fd, true);
  COROPACT_CHECK(nonblocking.has_value(), "KqueueStream: failed to set non-blocking mode");
  SuppressSigpipe(fd);

  channel_.SetTriggerMode(TriggerMode::kOneShot);
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

KqueueStream::KqueueStream(KqueueStream&& other) noexcept
    : loop_(PrepareMove(other)),
      socket_(std::move(other.socket_)),
      channel_(std::move(other.channel_)),
      peer_(other.peer_),
      lifecycle_(std::move(other.lifecycle_)) {
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

KqueueStream& KqueueStream::operator=(KqueueStream&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  KqueueLoop* other_loop = PrepareMove(other);
  COROPACT_CHECK(loop_ == nullptr || loop_ == other_loop,
                 "KqueueStream move requires both objects to use the same KqueueLoop");
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

KqueueStream::~KqueueStream() {
  if (loop_ == nullptr) {
    return;
  }
  RequireOwnerLoop();
  COROPACT_CHECK(pending_read_ == nullptr, "KqueueStream destroyed with a pending read");
  COROPACT_CHECK(pending_write_ == nullptr, "KqueueStream destroyed with a pending write");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
}

int KqueueStream::Release() noexcept {
  RequireOwnerLoop();
  COROPACT_CHECK(pending_read_ == nullptr, "KqueueStream cannot release with a pending read");
  COROPACT_CHECK(pending_write_ == nullptr, "KqueueStream cannot release with a pending write");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
  loop_ = nullptr;
  return socket_.Release();
}

KqueueStream::ReadSomeAwaiter KqueueStream::ReadSome(std::span<std::byte> buffer) noexcept {
  return ReadSomeAwaiter{*this, buffer};
}

KqueueStream::ReadSomeAwaiter KqueueStream::ReadSomeFor(std::span<std::byte> buffer,
                                                       time::Duration timeout) noexcept {
  return ReadSomeAwaiter{*this, buffer, timeout};
}

KqueueStream::ReadIntoAwaiter KqueueStream::ReadInto(net::Buffer buffer,
                                                     std::size_t reserve) noexcept {
  return ReadIntoAwaiter{*this, std::move(buffer), reserve};
}

coro::Task<Result<void>> KqueueStream::Shutdown() {
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

coro::Task<Result<void>> KqueueStream::Close() {
  RequireOwnerLoop();
  CloseNow();
  co_return Result<void>{};
}

void KqueueStream::HandleRead() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "KqueueStream::HandleRead called from wrong thread");
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

void KqueueStream::HandleWrite() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "KqueueStream::HandleWrite called from wrong thread");
  if (pending_write_ == nullptr) {
    return;
  }
  pending_write_->OnReady();
}

void KqueueStream::HandleClose() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "KqueueStream::HandleClose called from wrong thread");
  CompleteRead(Result<std::size_t>{0});
  CompleteWrite(std::unexpected(Errno(EPIPE)));
}

void KqueueStream::HandleError() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "KqueueStream::HandleError called from wrong thread");
  Error error = ErrorFromSocketErrorEvent(socket_.fd());
  CompleteRead(std::unexpected(error));
  CompleteWrite(std::unexpected(error));
}

void KqueueStream::CompleteRead(Result<std::size_t> result) {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "KqueueStream::CompleteRead called from wrong thread");
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
      COROPACT_CHECK(false, "KqueueStream::CompleteRead missing operation kind");
      return;
  }
  COROPACT_CHECK(result_authorized, "KqueueStream::CompleteRead result was already authorized");
  COROPACT_CHECK(state != nullptr, "KqueueStream::CompleteRead has no awaiter state");
  COROPACT_CHECK(state->TryAuthorizeRelease(),
                 "KqueueStream::CompleteRead release was not authorized after its result");

  void* released = std::exchange(pending_read_, nullptr);
  const PendingReadKind released_kind = std::exchange(pending_read_kind_, PendingReadKind::kNone);
  COROPACT_CHECK(released == awaiter && released_kind == kind,
                 "KqueueStream::CompleteRead pending slot changed during completion");

  // One-shot interest is already gone after delivery. A terminal path may
  // still see a re-armed filter from a WouldBlock retry; drop it so Close
  // does not leave a live registration behind.
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  COROPACT_CHECK(state->TryAuthorizeContinuation(),
                 "KqueueStream::CompleteRead continuation was not authorized after release");
  state->ScheduleContinuation();
}

void KqueueStream::CompleteWrite(Result<std::size_t> result) {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "KqueueStream::CompleteWrite called from wrong thread");
  WriteAllAwaiter* awaiter = pending_write_;
  if (awaiter == nullptr) {
    return;
  }
  COROPACT_CHECK(awaiter->CompleteResult(std::move(result)),
                 "KqueueStream::CompleteWrite result was already authorized");
  COROPACT_CHECK(awaiter->TryAuthorizeRelease(),
                 "KqueueStream::CompleteWrite release was not authorized after its result");

  WriteAllAwaiter* released = std::exchange(pending_write_, nullptr);
  COROPACT_CHECK(released == awaiter,
                 "KqueueStream::CompleteWrite pending slot changed during completion");
  if (channel_.IsWriting()) {
    channel_.DisableWriting();
  }
  COROPACT_CHECK(awaiter->TryAuthorizeContinuation(),
                 "KqueueStream::CompleteWrite continuation was not authorized after release");
  awaiter->ScheduleContinuation();
}

void KqueueStream::CloseNow() noexcept {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "KqueueStream::CloseNow called from wrong thread");
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

void KqueueStream::DetachChannel() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "KqueueStream::DetachChannel called from wrong thread");
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (channel_.IsRegistered()) {
    channel_.Remove();
  }
}

void KqueueStream::RequireOwnerLoop() const noexcept {
  COROPACT_CHECK(loop_ != nullptr, "KqueueStream operation has no owner KqueueLoop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "KqueueStream operation called from wrong KqueueLoop thread");
}

void KqueueStream::DispatchRead(void* context) noexcept {
  static_cast<KqueueStream*>(context)->HandleRead();
}

void KqueueStream::DispatchWrite(void* context) noexcept {
  static_cast<KqueueStream*>(context)->HandleWrite();
}

void KqueueStream::DispatchClose(void* context) noexcept {
  static_cast<KqueueStream*>(context)->HandleClose();
}

void KqueueStream::DispatchError(void* context) noexcept {
  static_cast<KqueueStream*>(context)->HandleError();
}

void KqueueStream::BindChannelCallbacks() noexcept {
  try {
    channel_.SetReadCallback(&KqueueStream::DispatchRead, this);
    channel_.SetWriteCallback(&KqueueStream::DispatchWrite, this);
    channel_.SetCloseCallback(&KqueueStream::DispatchClose, this);
    channel_.SetErrorCallback(&KqueueStream::DispatchError, this);
  } catch (...) {
    COROPACT_CHECK(false, "KqueueStream: failed to bind channel callbacks");
  }
}

void KqueueStream::ResetForMove() noexcept {
  COROPACT_CHECK(loop_ != nullptr, "KqueueStream move destination is not initialized");
  COROPACT_CHECK(loop_->IsInLoopThread(), "KqueueStream move called from wrong KqueueLoop thread");
  COROPACT_CHECK(pending_read_ == nullptr, "KqueueStream move destination has a pending read");
  COROPACT_CHECK(pending_write_ == nullptr, "KqueueStream move destination has a pending write");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
  socket_.Close();
}

KqueueLoop* KqueueStream::PrepareMove(KqueueStream& other) noexcept {
  COROPACT_CHECK(other.loop_ != nullptr, "KqueueStream move source is not initialized");
  COROPACT_CHECK(other.loop_->IsInLoopThread(),
                 "KqueueStream move called from wrong KqueueLoop thread");
  COROPACT_CHECK(other.pending_read_ == nullptr,
                 "KqueueStream cannot move with a pending read operation");
  COROPACT_CHECK(other.pending_write_ == nullptr,
                 "KqueueStream cannot move with a pending write operation");

  other.DetachChannel();
  LoopAccess::UnregisterShutdownParticipant(*other.loop_, other.shutdown_participant_);
  KqueueLoop* loop = std::exchange(other.loop_, nullptr);
  return loop;
}

void KqueueStream::DispatchLoopStop(void* context) noexcept {
  static_cast<KqueueStream*>(context)->CloseNow();
}

}  // namespace coropact::kqueue
