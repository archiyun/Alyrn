// SPDX-License-Identifier: MIT
#pragma once

#include <sys/uio.h>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <span>

#include "alyrn/detail/net/socket.h"
#include "alyrn/detail/net/stream_lifecycle.h"
#include "alyrn/detail/operation/scheduler_continuation.h"
#include "alyrn/detail/operation/single_result_lifecycle.h"
#include "alyrn/detail/epoll/channel.h"
#include "alyrn/detail/epoll/loop_shutdown.h"
#include "alyrn/detail/epoll/op_hook.h"
#include "alyrn/detail/epoll/result_state.h"
#include "alyrn/detail/utils/macros.h"
#include "alyrn/net/buffer.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/read_into.h"
#include "alyrn/epoll/loop.h"
#include "alyrn/epoll/options.h"
#include "alyrn/result.h"
#include "alyrn/task.h"
#include "alyrn/time/clock.h"

namespace alyrn::epoll {

struct StreamOptions {
  // ET keeps successful-read interest armed. LT keeps it armed for an
  // immediate re-arm and lazily removes it if readiness arrives without a
  // pending read. Both modes still probe the non-blocking socket first.
  TriggerMode trigger_mode{TriggerMode::kEdgeTriggered};
};

class Stream {
private:
  class ReadSomeAwaiter;
  class ReadIntoAwaiter;
  class ReadAwaiterState;
  class WriteAllAwaiter;

public:
  ALYRN_DELETE_COPY(Stream);

  // Read/write methods intentionally return private awaiter types. Call them
  // directly with co_await (or keep the result in auto); their registration,
  // result storage, and cancellation protocol are not a stream interface.

  Stream(Loop* loop, int fd, net::Endpoint peer = net::Endpoint(0), StreamOptions options = {});
  ~Stream();

  // Moves are loop-affine: the source must be used from its owning loop
  // thread and must not have a pending read or write operation.
  Stream(Stream&& other) noexcept;
  Stream& operator=(Stream&& other) noexcept;

  [[nodiscard]]
  ReadSomeAwaiter ReadSome(std::span<std::byte> buffer) noexcept;
  [[nodiscard]]
  ReadIntoAwaiter ReadInto(net::Buffer buffer, std::size_t reserve = 4096) noexcept;
  [[nodiscard]]
  ReadSomeAwaiter ReadSomeFor(std::span<std::byte> buffer, time::Duration timeout) noexcept;
  [[nodiscard]]
  WriteAllAwaiter WriteAll(std::span<const std::byte> buffer) noexcept;
  [[nodiscard]]
  // Legacy alias for CloseWrite().
  ::alyrn::Task<Result<void>> Shutdown() noexcept;
  [[nodiscard]]
  ::alyrn::Task<Result<void>> Close() noexcept;

  // Stream operations and destruction are loop-affine. The coroutine must
  // reach await_suspend() on this stream's owning Loop; a foreign thread
  // violates the runtime contract and terminates through ALYRN_CHECK in
  // every build configuration.

  [[nodiscard]]
  Result<net::Endpoint> LocalAddr() const noexcept;

  [[nodiscard]]
  const net::Endpoint& RemoteAddr() const noexcept {
    return peer_;
  }

  [[nodiscard]]
  Result<void> SetNoDelay(bool enabled) const noexcept;

  [[nodiscard]]
  Result<void> SetKeepAlive(bool enabled) const noexcept;

  [[nodiscard]]
  Result<void> SetKeepAlivePeriod(time::Duration period) const noexcept;

  [[nodiscard]]
  Result<void> SetReadBuffer(std::size_t bytes) const noexcept;

  [[nodiscard]]
  Result<void> SetWriteBuffer(std::size_t bytes) const noexcept;

  [[nodiscard]]
  // Shuts down local reception while keeping the descriptor and write side.
  ::alyrn::Task<Result<void>> CloseRead() noexcept;

  [[nodiscard]]
  // Shuts down local transmission while keeping the descriptor and read side.
  ::alyrn::Task<Result<void>> CloseWrite() noexcept;

  // Native extensions such as RecvSource may borrow this descriptor
  // while this stream retains ownership. They must run on the owning loop and
  // finish before Close() releases the descriptor.
  [[nodiscard]]
  int Fd() const noexcept {
    return socket_.fd();
  }

  [[nodiscard]]
  Loop* OwnerLoop() const noexcept {
    return loop_;
  }

private:
  // Private implementation vocabulary. Keep these aliases class-scoped: a
  // public-header-level using-directive would leak epoll::detail names into
  // every includer's alyrn::epoll namespace.
  using Channel = detail::Channel;
  using LoopShutdownParticipant = detail::LoopShutdownParticipant;
  using SchedulerContinuation = ::alyrn::detail::operation::SchedulerContinuation;
  using SingleResultLifecycle = ::alyrn::detail::operation::SingleResultLifecycle;
  using IoResultState = detail::IoResultState;

  template <class Awaiter>
  using OperationHook = detail::OperationHook<Awaiter>;

  void HandleRead();
  void HandleWrite();
  void HandleClose();
  void HandleError();

  static void DispatchRead(void* context) noexcept;
  static void DispatchWrite(void* context) noexcept;
  static void DispatchClose(void* context) noexcept;
  static void DispatchError(void* context) noexcept;

  void CompleteRead(Result<std::size_t> result);
  void CompleteWrite(Result<std::size_t> result);
  void CloseNow() noexcept;
  void DetachChannel();
  void RequireOwnerLoop() const noexcept;
  void BindChannelCallbacks() noexcept;
  void ResetForMove() noexcept;
  static Loop* PrepareMove(Stream& other) noexcept;
  static void DispatchLoopStop(void* context) noexcept;

  Loop* loop_;
  net::Socket socket_;
  Channel channel_;
  net::Endpoint peer_;
  enum class PendingReadKind : std::uint8_t {
    kNone,
    kReadSome,
    kReadInto,
  };
  void* pending_read_{nullptr};
  WriteAllAwaiter* pending_write_{nullptr};
  PendingReadKind pending_read_kind_{PendingReadKind::kNone};
  net::detail::StreamLifecycle lifecycle_;
  LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

// Shared state for single-result read awaiters. It owns the common stream and
// continuation protocol; derived awaiters retain buffer ownership and terminal
// cleanup, which differ between borrowed, buffered, and owned reads.
class Stream::ReadAwaiterState {
public:
  // Completion dispatch calls these in the lifecycle order after the derived
  // awaiter has stored its result and before it schedules the continuation.
  [[nodiscard]]
  bool TryAuthorizeRelease() noexcept;
  [[nodiscard]]
  bool TryAuthorizeContinuation() noexcept;
  void ScheduleContinuation() noexcept;

protected:
  explicit ReadAwaiterState(Stream& stream) noexcept : stream_(&stream) {}

  [[nodiscard]]
  bool BeginRead(std::coroutine_handle<> continuation) noexcept;
  void SuspendForRead(void* awaiter, PendingReadKind kind) noexcept;

  void ArmReadTimeout(time::Duration timeout, void* awaiter, time::TimerId& timer) noexcept;
  void CancelReadTimeout(time::TimerId& timer) noexcept;

  [[nodiscard]]
  bool TryAuthorizeResult() noexcept;
  void CompleteInline(Result<std::size_t> result) noexcept;
  void CompleteStoredInline() noexcept;
  void SetResult(Result<std::size_t> result) noexcept;

  [[nodiscard]]
  Result<std::size_t> TakeResult() noexcept;

  Stream* stream_;
  SchedulerContinuation continuation_;
  SingleResultLifecycle lifecycle_;
  IoResultState result_;
};

class Stream::ReadSomeAwaiter final : public ReadAwaiterState,
                                      public OperationHook<Stream::ReadSomeAwaiter> {
public:
  ALYRN_DELETE_COPY_MOVE(ReadSomeAwaiter);

  ReadSomeAwaiter(Stream& stream, std::span<std::byte> buffer,
                  time::Duration timeout = time::Duration::zero()) noexcept
      : ReadAwaiterState(stream), buffer_(buffer), timeout_(timeout) {}

  bool await_ready() const noexcept { return false; }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<std::size_t> await_resume() noexcept;

private:
  friend OperationHook<ReadSomeAwaiter>;

  bool CompleteResultImpl(Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;

  std::span<std::byte> buffer_;
  time::Duration timeout_;
  time::TimerId timer_;
};

// Keeps the short-write loop in the caller's coroutine frame. The physical
// write operation is private; callers observe only complete-or-error WriteAll.
class Stream::WriteAllAwaiter final : public OperationHook<Stream::WriteAllAwaiter> {
public:
  ALYRN_DELETE_COPY_MOVE(WriteAllAwaiter);

  WriteAllAwaiter(Stream& stream, std::span<const std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  bool await_ready() const noexcept { return false; }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<void> await_resume() noexcept;

private:
  friend class Stream;
  friend OperationHook<WriteAllAwaiter>;

  void CompleteInline(Result<std::size_t> result) noexcept;
  [[nodiscard]] bool CompleteResultImpl(Result<std::size_t> result) noexcept;
  [[nodiscard]] bool TryAuthorizeRelease() noexcept;
  [[nodiscard]] bool TryAuthorizeContinuation() noexcept;
  void ScheduleContinuation() noexcept;
  void OnReadyImpl() noexcept;

  Stream* stream_;
  std::span<const std::byte> buffer_;
  SchedulerContinuation continuation_;
  SingleResultLifecycle lifecycle_;
  IoResultState result_;
};

// Owns the destination buffer while a read is pending and returns it on every
// terminal path, so callers cannot invalidate its storage while the backend
// may still access it.
class Stream::ReadIntoAwaiter : public ReadAwaiterState,
                                public OperationHook<Stream::ReadIntoAwaiter> {
public:
  ALYRN_DELETE_COPY_MOVE(ReadIntoAwaiter);

  ReadIntoAwaiter(Stream& stream, net::Buffer buffer, std::size_t reserve) noexcept;

  bool await_ready() const noexcept { return false; }
  [[nodiscard]] bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  net::ReadIntoOutcome await_resume() noexcept;

private:
  friend OperationHook<ReadIntoAwaiter>;

  bool CompleteResultImpl(Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;
  bool PrepareReservation() noexcept;
  void FinishAttempt(Result<std::size_t> result) noexcept;

  net::Buffer buffer_;
  std::size_t reserve_;
  bool reservation_active_{false};
};

}  // namespace alyrn::epoll
