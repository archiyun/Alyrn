// SPDX-License-Identifier: MIT
#include "alyrn/epoll/stream.h"

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
#include "alyrn/epoll/detail/loop_access.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"

namespace alyrn::epoll {

using detail::LoopAccess;

namespace {

constexpr std::size_t kRecvMaxIov = 16;

constexpr bool IsWouldBlock(int error) noexcept { return error == EAGAIN || error == EWOULDBLOCK; }

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
  return RetryNonBlockingIo(
      [fd, buffer]() noexcept { return ::send(fd, buffer.data(), buffer.size(), MSG_NOSIGNAL); });
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
  if (!count.HasValue()) {
    return IoAttempt::Failed(count.Error());
  }

  return RetryNonBlockingIo([fd, buffers, iov_count = *count]() noexcept {
    return ::readv(fd, buffers.data(), iov_count);
  });
}

// Called only after a epoll error event. SO_ERROR == 0 is inconsistent with
// that event, so use EIO as a stable error result rather than reporting errno 0.
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

}  // namespace

// --- ReadAwaiterState ---
bool Stream::ReadAwaiterState::BeginRead(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    CompleteInline(std::unexpected(Errno(ECANCELED)));
    return false;
  }
  auto valid = stream_->lifecycle_.ValidateRead();
  if (!valid.HasValue()) {
    CompleteInline(std::unexpected(valid.Error()));
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
  if (!stream_->channel_.IsReading()) {
    stream_->channel_.EnableReading();
  }
  if (!stream_->ArmReadDeadline()) {
    stream_->CompleteRead(std::unexpected(Errno(ENOMEM)));
  }
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

Stream::ReadAwaiterState::~ReadAwaiterState() {
  if (stream_ == nullptr || stream_->pending_read_ != this) {
    return;
  }
  stream_->CancelReadDeadline();
  stream_->pending_read_ = nullptr;
  stream_->pending_read_kind_ = PendingReadKind::kNone;
  if (stream_->channel_.IsReading()) {
    stream_->channel_.DisableReading();
  }
  stream_ = nullptr;
}

void Stream::ReadAwaiterState::CompleteInline(Result<std::size_t> result) noexcept {
  result_.SetResult(result);
  CompleteStoredInline();
}

void Stream::ReadAwaiterState::CompleteStoredInline() noexcept {
  ALYRN_CHECK(TryAuthorizeResult(), "Epoll read result was authorized twice");
  ALYRN_CHECK(TryAuthorizeRelease(), "Epoll read release was not authorized after its result");
}

void Stream::ReadAwaiterState::SetResult(Result<std::size_t> result) noexcept {
  result_.SetResult(result);
}

void Stream::ReadAwaiterState::ScheduleContinuation() noexcept { continuation_.Schedule(); }

Result<std::size_t> Stream::ReadAwaiterState::TakeResult() noexcept { return result_.Take(); }

// --- ReadAwaiter ---
bool Stream::ReadAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (!BeginRead(continuation)) {
    return false;
  }

  auto [state, result] = TryRead(stream_->socket_.fd(), buffer_);
  if (state != IoAttemptState::kWouldBlock) {
    CompleteInline(result);
    return false;
  }

  SuspendForRead(this, Stream::PendingReadKind::kRead);
  return true;
}

Result<std::size_t> Stream::ReadAwaiter::await_resume() noexcept { return TakeResult(); }

bool Stream::ReadAwaiter::CompleteResultImpl(Result<std::size_t> result) noexcept {
  if (!TryAuthorizeResult()) {
    return false;
  }
  stream_ = nullptr;
  SetResult(result);
  return true;
}

void Stream::ReadAwaiter::OnReadyImpl() noexcept {
  auto [state, result] = TryRead(stream_->socket_.fd(), buffer_);
  if (state == IoAttemptState::kWouldBlock) {
    return;
  }
  stream_->CompleteRead(result);
}

// --- RecvAwaiter ---
Stream::RecvAwaiter::RecvAwaiter(Stream& stream, net::Buffer buffer, std::size_t reserve) noexcept
    : ReadAwaiterState(stream), buffer_(std::move(buffer)), reserve_(reserve) {}

Stream::RecvAwaiter::~RecvAwaiter() {
  if (reservation_active_) {
    buffer_.AbortWrite();
    reservation_active_ = false;
  }
}

bool Stream::RecvAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (!BeginRead(continuation)) {
    return false;
  }

  if (!PrepareReservation()) {
    CompleteStoredInline();
    return false;
  }

  std::array<iovec, kRecvMaxIov> iovs;
  auto [state, result] = TryReadv(stream_->socket_.fd(), buffer_.ReservedWriteIov(iovs));
  if (state != IoAttemptState::kWouldBlock) {
    FinishAttempt(result);
    CompleteStoredInline();
    return false;
  }

  SuspendForRead(this, PendingReadKind::kRecv);
  return true;
}

net::RecvOutcome Stream::RecvAwaiter::await_resume() noexcept {
  return {
      .result = TakeResult(),
      .buffer = std::move(buffer_),
  };
}

bool Stream::RecvAwaiter::CompleteResultImpl(Result<std::size_t> result) noexcept {
  if (!TryAuthorizeResult()) {
    return false;
  }
  FinishAttempt(result);
  stream_ = nullptr;
  return true;
}

void Stream::RecvAwaiter::OnReadyImpl() noexcept {
  std::array<iovec, kRecvMaxIov> iovs;
  auto [state, result] = TryReadv(stream_->socket_.fd(), buffer_.ReservedWriteIov(iovs));
  if (state == IoAttemptState::kWouldBlock) {
    return;
  }
  stream_->CompleteRead(result);
}

bool Stream::RecvAwaiter::PrepareReservation() noexcept {
  try {
    std::array<iovec, kRecvMaxIov> iovs;
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

void Stream::RecvAwaiter::FinishAttempt(Result<std::size_t> result) noexcept {
  ALYRN_CHECK(reservation_active_, "RecvAwaiter completion without a buffer reservation");
  if (result.HasValue()) {
    buffer_.CommitWrite(*result);
  } else {
    buffer_.AbortWrite();
  }
  reservation_active_ = false;
  result_.SetResult(result);
}

Stream::RecvCopyAwaiter::RecvCopyAwaiter(Stream& stream) noexcept
    : ReadAwaiterState(stream), reserve_(net::Buffer::kDefaultBlockSize) {}

Stream::RecvCopyAwaiter::~RecvCopyAwaiter() {
  if (reservation_active_) {
    buffer_.AbortWrite();
    reservation_active_ = false;
  }
}

bool Stream::RecvCopyAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (!BeginRead(continuation)) {
    return false;
  }

  if (!PrepareReservation()) {
    CompleteStoredInline();
    return false;
  }

  std::array<iovec, kRecvMaxIov> iovs;
  auto [state, result] = TryReadv(stream_->socket_.fd(), buffer_.ReservedWriteIov(iovs));
  if (state != IoAttemptState::kWouldBlock) {
    FinishAttempt(result);
    CompleteStoredInline();
    return false;
  }

  SuspendForRead(this, PendingReadKind::kRecvCopy);
  return true;
}

Result<net::Buffer> Stream::RecvCopyAwaiter::await_resume() noexcept {
  auto result = TakeResult();
  if (!result.HasValue()) {
    return std::unexpected(result.Error());
  }
  return std::move(buffer_);
}

bool Stream::RecvCopyAwaiter::CompleteResultImpl(Result<std::size_t> result) noexcept {
  if (!TryAuthorizeResult()) {
    return false;
  }
  FinishAttempt(result);
  stream_ = nullptr;
  return true;
}

void Stream::RecvCopyAwaiter::OnReadyImpl() noexcept {
  std::array<iovec, kRecvMaxIov> iovs;
  auto [state, result] = TryReadv(stream_->socket_.fd(), buffer_.ReservedWriteIov(iovs));
  if (state == IoAttemptState::kWouldBlock) {
    return;
  }
  stream_->CompleteRead(result);
}

bool Stream::RecvCopyAwaiter::PrepareReservation() noexcept {
  try {
    std::array<iovec, kRecvMaxIov> iovs;
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

void Stream::RecvCopyAwaiter::FinishAttempt(Result<std::size_t> result) noexcept {
  ALYRN_CHECK(reservation_active_, "RecvCopyAwaiter completion without a buffer reservation");
  if (result.HasValue()) {
    buffer_.CommitWrite(*result);
  } else {
    buffer_.AbortWrite();
  }
  reservation_active_ = false;
  result_.SetResult(result);
}

// --- WriteAwaiter ---
Stream::WriteAwaiter Stream::Write(std::span<const std::byte> buffer) noexcept {
  return WriteAwaiter{*this, buffer};
}

bool Stream::WriteAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  stream_->RequireOwnerLoop();
  if (stream_->loop_->State() == backend::LoopState::kStopping ||
      stream_->loop_->State() == backend::LoopState::kStopped) {
    CompleteInline(std::unexpected(Errno(ECANCELED)));
    return false;
  }
  auto valid = stream_->lifecycle_.ValidateWrite();
  if (!valid.HasValue()) {
    CompleteInline(std::unexpected(valid.Error()));
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
      if (!stream_->channel_.IsWriting()) {
        stream_->channel_.EnableWriting();
      }
      if (!stream_->ArmWriteDeadline()) {
        stream_->CompleteWrite(std::unexpected(Errno(ENOMEM)));
      }
      return true;
    }
    if (!result.HasValue()) {
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

Result<void> Stream::WriteAwaiter::await_resume() noexcept {
  auto result = result_.Take();
  if (!result.HasValue()) {
    return std::unexpected(result.Error());
  }
  return Result<void>{};
}

void Stream::WriteAwaiter::CompleteInline(Result<std::size_t> result) noexcept {
  result_.SetResult(result);
  ALYRN_CHECK(lifecycle_.TryAuthorizeResult(), "Epoll write result was authorized twice");
  ALYRN_CHECK(lifecycle_.TryAuthorizeRelease(),
              "Epoll write release was not authorized after its result");
}

Stream::WriteAwaiter::~WriteAwaiter() {
  if (stream_ == nullptr || stream_->pending_write_ != this) {
    return;
  }
  stream_->CancelWriteDeadline();
  stream_->pending_write_ = nullptr;
  if (stream_->channel_.IsWriting()) {
    stream_->channel_.DisableWriting();
  }
  stream_ = nullptr;
}

bool Stream::WriteAwaiter::CompleteResultImpl(Result<std::size_t> result) noexcept {
  if (!lifecycle_.TryAuthorizeResult()) {
    return false;
  }
  result_.SetResult(result);
  stream_ = nullptr;
  return true;
}

bool Stream::WriteAwaiter::TryAuthorizeRelease() noexcept {
  return lifecycle_.TryAuthorizeRelease();
}

bool Stream::WriteAwaiter::TryAuthorizeContinuation() noexcept {
  return lifecycle_.TryAuthorizeContinuation();
}

void Stream::WriteAwaiter::ScheduleContinuation() noexcept { continuation_.Schedule(); }

void Stream::WriteAwaiter::OnReadyImpl() noexcept {
  while (!buffer_.empty()) {
    auto [state, result] = TryWrite(stream_->socket_.fd(), buffer_);
    if (state == IoAttemptState::kWouldBlock) {
      return;
    }
    if (!result.HasValue()) {
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
  [[maybe_unused]] auto nonblocking = net::SetNonBlocking(fd, true);
  ALYRN_CHECK(nonblocking.HasValue(), "Stream: failed to set non-blocking mode");

  // A stream keeps read interest across successful reads. Edge-triggered
  // delivery avoids the level-triggered disable/re-enable epoll_ctl pair on
  // every keep-alive request; every Read still probes the socket first.
  channel_.SetEdgeTriggered(options.trigger_mode == TriggerMode::kEdgeTriggered);
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

Stream::Stream(Stream&& other) noexcept
    : loop_(PrepareMove(other)),
      socket_(std::move(other.socket_)),
      channel_(std::move(other.channel_)),
      peer_(other.peer_),
      read_deadline_(std::move(other.read_deadline_)),
      write_deadline_(std::move(other.write_deadline_)),
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
  read_deadline_ = std::move(other.read_deadline_);
  write_deadline_ = std::move(other.write_deadline_);
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
  CloseNow();
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
}

Stream::ReadAwaiter Stream::Read(std::span<std::byte> buffer) noexcept {
  return ReadAwaiter{*this, buffer};
}

Stream::RecvCopyAwaiter Stream::Recv() noexcept { return RecvCopyAwaiter{*this}; }

Stream::RecvAwaiter Stream::Recv(net::Buffer buffer, std::size_t reserve) noexcept {
  return RecvAwaiter{*this, std::move(buffer), reserve};
}

coro::Task<Result<void>> Stream::Shutdown() noexcept { return CloseWrite(); }

coro::Task<Result<void>> Stream::CloseWrite() noexcept {
  RequireOwnerLoop();
  if (socket_.fd() < 0) {
    co_return std::unexpected(Errno(EBADF));
  }
  auto prepare = lifecycle_.PrepareShutdown(pending_write_ != nullptr);
  if (!prepare.HasValue()) {
    co_return std::unexpected(prepare.Error());
  }
  if (!*prepare) {
    co_return Result<void>{};
  }
  auto shutdown = socket_.ShutdownWrite();
  if (!shutdown.HasValue()) {
    lifecycle_.AbortShutdownPreparation();
    co_return std::unexpected(shutdown.Error());
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
  if (!prepare.HasValue()) {
    co_return std::unexpected(prepare.Error());
  }
  if (!*prepare) {
    co_return Result<void>{};
  }
  auto close_read = socket_.ShutdownRead();
  if (!close_read.HasValue()) {
    lifecycle_.AbortCloseReadPreparation();
    co_return std::unexpected(close_read.Error());
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

Result<void> Stream::SetDeadline(std::optional<time::Deadline> deadline) noexcept {
  auto read_result = SetReadDeadline(deadline);
  if (!read_result.HasValue()) {
    return read_result;
  }
  return SetWriteDeadline(deadline);
}

Result<void> Stream::SetReadDeadline(std::optional<time::Deadline> deadline) noexcept {
  RequireOwnerLoop();
  CancelReadDeadline();
  read_deadline_ = deadline;
  if (pending_read_ != nullptr && !ArmReadDeadline()) {
    CompleteRead(std::unexpected(Errno(ENOMEM)));
    return std::unexpected(Errno(ENOMEM));
  }
  return {};
}

Result<void> Stream::SetWriteDeadline(std::optional<time::Deadline> deadline) noexcept {
  RequireOwnerLoop();
  CancelWriteDeadline();
  write_deadline_ = deadline;
  if (pending_write_ != nullptr && !ArmWriteDeadline()) {
    CompleteWrite(std::unexpected(Errno(ENOMEM)));
    return std::unexpected(Errno(ENOMEM));
  }
  return {};
}

void Stream::HandleRead() {
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
    case PendingReadKind::kRead:
      static_cast<ReadAwaiter*>(pending_read_)->OnReady();
      return;
    case PendingReadKind::kRecv:
      static_cast<RecvAwaiter*>(pending_read_)->OnReady();
      return;
    case PendingReadKind::kRecvCopy:
      static_cast<RecvCopyAwaiter*>(pending_read_)->OnReady();
      return;
    case PendingReadKind::kNone:
      return;
  }
}

void Stream::HandleWrite() {
  if (pending_write_ == nullptr) {
    return;
  }
  pending_write_->OnReady();
}

void Stream::HandleClose() {
  CompleteRead(Result<std::size_t>{0});
  CompleteWrite(std::unexpected(Errno(EPIPE)));
}

void Stream::HandleError() {
  Error error = ErrorFromSocketErrorEvent(socket_.fd());
  CompleteRead(std::unexpected(error));
  CompleteWrite(std::unexpected(error));
}

void Stream::CompleteRead(Result<std::size_t> result) {
  void* awaiter = pending_read_;
  const PendingReadKind kind = pending_read_kind_;
  if (awaiter == nullptr) {
    return;
  }
  CancelReadDeadline();

  const bool terminal_result = !result.HasValue() || *result == 0;
  ReadAwaiterState* state = nullptr;
  bool result_authorized = false;
  switch (kind) {
    case PendingReadKind::kRead:
      state = static_cast<ReadAwaiter*>(awaiter);
      result_authorized = static_cast<ReadAwaiter*>(awaiter)->CompleteResult(result);
      break;
    case PendingReadKind::kRecv:
      state = static_cast<RecvAwaiter*>(awaiter);
      result_authorized = static_cast<RecvAwaiter*>(awaiter)->CompleteResult(result);
      break;
    case PendingReadKind::kRecvCopy:
      state = static_cast<RecvCopyAwaiter*>(awaiter);
      result_authorized = static_cast<RecvCopyAwaiter*>(awaiter)->CompleteResult(result);
      break;
    case PendingReadKind::kNone:
      ALYRN_CHECK(false, "Stream::CompleteRead missing operation kind");
      return;
  }
  ALYRN_CHECK(result_authorized, "Stream::CompleteRead result was already authorized");
  ALYRN_CHECK(state != nullptr, "Stream::CompleteRead has no awaiter state");
  ALYRN_CHECK(state->TryAuthorizeRelease(),
              "Stream::CompleteRead release was not authorized after its result");

  // The stream slot is an operation resource. Release it only after the
  // result is fixed, but before continuation authorization: resumed code may
  // immediately submit the next read.
  void* released = std::exchange(pending_read_, nullptr);
  const PendingReadKind released_kind = std::exchange(pending_read_kind_, PendingReadKind::kNone);
  ALYRN_CHECK(released == awaiter && released_kind == kind,
              "Stream::CompleteRead pending slot changed during completion");

  // Successful reads keep interest armed in both modes so a continuation can
  // immediately submit the next read without an epoll_ctl pair. LT disarms
  // lazily in HandleRead when readiness arrives without a pending operation.
  // Terminal results remove the interest in both modes.
  if (terminal_result) {
    if (channel_.IsReading()) {
      channel_.DisableReading();
    }
  }
  ALYRN_CHECK(state->TryAuthorizeContinuation(),
              "Stream::CompleteRead continuation was not authorized after release");
  state->ScheduleContinuation();
}

void Stream::CompleteWrite(Result<std::size_t> result) {
  WriteAwaiter* awaiter = pending_write_;
  if (awaiter == nullptr) {
    return;
  }
  CancelWriteDeadline();
  ALYRN_CHECK(awaiter->CompleteResult(result),
              "Stream::CompleteWrite result was already authorized");
  ALYRN_CHECK(awaiter->TryAuthorizeRelease(),
              "Stream::CompleteWrite release was not authorized after its result");

  WriteAwaiter* released = std::exchange(pending_write_, nullptr);
  ALYRN_CHECK(released == awaiter, "Stream::CompleteWrite pending slot changed during completion");
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
  if (!close_prepared.HasValue() || !*close_prepared) {
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

bool Stream::ArmReadDeadline() noexcept {
  if (pending_read_ == nullptr || !read_deadline_.has_value()) {
    return true;
  }

  if (*read_deadline_ <= time::SteadyNow()) {
    CompleteRead(std::unexpected(Errno(ETIMEDOUT)));
    return true;
  }

  const std::uint64_t generation = ++read_timer_generation_;
  try {
    read_timer_ = loop_->RunAt(*read_deadline_, [this, generation] {
      HandleReadDeadline(generation);
    });
    return true;
  } catch (...) {
    return false;
  }
}

bool Stream::ArmWriteDeadline() noexcept {
  if (pending_write_ == nullptr || !write_deadline_.has_value()) {
    return true;
  }

  if (*write_deadline_ <= time::SteadyNow()) {
    CompleteWrite(std::unexpected(Errno(ETIMEDOUT)));
    return true;
  }

  const std::uint64_t generation = ++write_timer_generation_;
  try {
    write_timer_ = loop_->RunAt(*write_deadline_, [this, generation] {
      HandleWriteDeadline(generation);
    });
    return true;
  } catch (...) {
    return false;
  }
}

void Stream::CancelReadDeadline() noexcept {
  ++read_timer_generation_;
  const auto timer = std::exchange(read_timer_, time::TimerId{});
  if (timer.Valid()) {
    loop_->Cancel(timer);
  }
}

void Stream::CancelWriteDeadline() noexcept {
  ++write_timer_generation_;
  const auto timer = std::exchange(write_timer_, time::TimerId{});
  if (timer.Valid()) {
    loop_->Cancel(timer);
  }
}

void Stream::HandleReadDeadline(std::uint64_t generation) noexcept {
  if (generation != read_timer_generation_) {
    return;
  }
  read_timer_ = {};
  if (pending_read_ != nullptr) {
    CompleteRead(std::unexpected(Errno(ETIMEDOUT)));
  }
}

void Stream::HandleWriteDeadline(std::uint64_t generation) noexcept {
  if (generation != write_timer_generation_) {
    return;
  }
  write_timer_ = {};
  if (pending_write_ != nullptr) {
    CompleteWrite(std::unexpected(Errno(ETIMEDOUT)));
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
  ALYRN_CHECK(other.pending_read_ == nullptr, "Stream cannot move with a pending read operation");
  ALYRN_CHECK(other.pending_write_ == nullptr, "Stream cannot move with a pending write operation");

  other.DetachChannel();
  LoopAccess::UnregisterShutdownParticipant(*other.loop_, other.shutdown_participant_);
  Loop* loop = std::exchange(other.loop_, nullptr);
  return loop;
}

void Stream::DispatchLoopStop(void* context) noexcept { static_cast<Stream*>(context)->CloseNow(); }

}  // namespace alyrn::epoll
