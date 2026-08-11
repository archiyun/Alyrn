// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <sys/uio.h>

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <span>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/net/buffer.h"
#include "coropact/net/detail/stream_lifecycle.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/read_into.h"
#include "coropact/net/socket.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/operation/detail/single_result_lifecycle.h"
#include "coropact/reactor/detail/channel.h"
#include "coropact/reactor/detail/loop_shutdown.h"
#include "coropact/reactor/detail/op_hook.h"
#include "coropact/reactor/detail/result_state.h"
#include "coropact/reactor/loop.h"
#include "coropact/reactor/options.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

struct ReactorStreamOptions {
  // ET keeps successful-read interest armed. LT keeps it armed for an
  // immediate re-arm and lazily removes it if readiness arrives without a
  // pending read. Both modes still probe the non-blocking socket first.
  TriggerMode trigger_mode{TriggerMode::kEdgeTriggered};
};

class ReactorStream {
private:
  class ReadSomeAwaiter;
  class ReadIntoAwaiter;
  class ReadAwaiterState;
  class WriteAllAwaiter;

public:
  COROPACT_DELETE_COPY(ReactorStream);

  // Read/write methods intentionally return private awaiter types. Call them
  // directly with co_await (or keep the result in auto); their registration,
  // result storage, and cancellation protocol are not a stream interface.

  ReactorStream(EventLoop* loop, int fd, net::Endpoint peer = net::Endpoint(0),
                ReactorStreamOptions options = {});
  ~ReactorStream();

  // Moves are loop-affine: the source must be used from its owning loop
  // thread and must not have a pending read or write operation.
  ReactorStream(ReactorStream&& other) noexcept;
  ReactorStream& operator=(ReactorStream&& other) noexcept;

  [[nodiscard]]
  ReadSomeAwaiter ReadSome(std::span<std::byte> buffer) noexcept;
  [[nodiscard]]
  ReadIntoAwaiter ReadInto(net::Buffer buffer, std::size_t reserve = 4096) noexcept;
  [[nodiscard]]
  ReadSomeAwaiter ReadSomeFor(std::span<std::byte> buffer,
                              std::chrono::milliseconds timeout) noexcept;
  [[nodiscard]]
  WriteAllAwaiter WriteAll(std::span<const std::byte> buffer) noexcept;
  coro::Task<base::Result<void>> Shutdown();
  coro::Task<base::Result<void>> Close();

  // Stream operations and destruction are loop-affine. The coroutine must
  // reach await_suspend() on this stream's owning EventLoop; a foreign thread
  // violates the runtime contract and terminates through COROPACT_CHECK in
  // every build configuration.

  [[nodiscard]]
  const net::Endpoint& PeerAddress() const noexcept {
    return peer_;
  }

  // Native extensions such as ReactorRecvSource may borrow this descriptor
  // while this stream retains ownership. They must run on the owning loop and
  // finish before Close() releases the descriptor.
  [[nodiscard]]
  int Fd() const noexcept {
    return socket_.fd();
  }

  [[nodiscard]]
  EventLoop* Loop() const noexcept {
    return loop_;
  }

private:
  // Private implementation vocabulary. Keep these aliases class-scoped: a
  // public-header-level using-directive would leak reactor::detail names into
  // every includer's coropact::reactor namespace.
  using Channel = detail::Channel;
  using LoopShutdownParticipant = detail::LoopShutdownParticipant;
  using SchedulerContinuation = operation::detail::SchedulerContinuation;
  using SingleResultLifecycle = operation::detail::SingleResultLifecycle;
  using IoResultState = detail::ReactorIoResultState;

  template <class Awaiter>
  using OperationHook = detail::ReactorOperationHook<Awaiter>;

  void HandleRead();
  void HandleWrite();
  void HandleClose();
  void HandleError();

  static void DispatchRead(void* context) noexcept;
  static void DispatchWrite(void* context) noexcept;
  static void DispatchClose(void* context) noexcept;
  static void DispatchError(void* context) noexcept;

  void CompleteRead(base::Result<std::size_t> result);
  void CompleteWrite(base::Result<std::size_t> result);
  void CloseNow() noexcept;
  void DetachChannel();
  void RequireOwnerLoop() const noexcept;
  void BindChannelCallbacks() noexcept;
  void ResetForMove() noexcept;
  static EventLoop* PrepareMove(ReactorStream& other) noexcept;
  static void DispatchLoopStop(void* context) noexcept;

  EventLoop* loop_;
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
class ReactorStream::ReadAwaiterState {
public:
  // Completion dispatch calls these in the lifecycle order after the derived
  // awaiter has stored its result and before it schedules the continuation.
  [[nodiscard]]
  bool TryAuthorizeRelease() noexcept;
  [[nodiscard]]
  bool TryAuthorizeContinuation() noexcept;
  void ScheduleContinuation() noexcept;

protected:
  explicit ReadAwaiterState(ReactorStream& stream) noexcept : stream_(&stream) {}

  [[nodiscard]]
  bool BeginRead(std::coroutine_handle<> continuation) noexcept;
  void SuspendForRead(void* awaiter, PendingReadKind kind) noexcept;

  void ArmReadTimeout(std::chrono::milliseconds timeout, void* awaiter,
                      time::TimerId& timer) noexcept;
  void CancelReadTimeout(time::TimerId& timer) noexcept;

  [[nodiscard]]
  bool TryAuthorizeResult() noexcept;
  void CompleteInline(base::Result<std::size_t> result) noexcept;
  void CompleteStoredInline() noexcept;
  void SetResult(base::Result<std::size_t> result) noexcept;

  [[nodiscard]]
  base::Result<std::size_t> TakeResult() noexcept;

  ReactorStream* stream_;
  SchedulerContinuation continuation_;
  SingleResultLifecycle lifecycle_;
  IoResultState result_;
};

class ReactorStream::ReadSomeAwaiter final : public ReadAwaiterState,
                                             public OperationHook<ReactorStream::ReadSomeAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(ReadSomeAwaiter);

  ReadSomeAwaiter(ReactorStream& stream, std::span<std::byte> buffer,
                  std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) noexcept
      : ReadAwaiterState(stream), buffer_(buffer), timeout_(timeout) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  friend OperationHook<ReadSomeAwaiter>;

  bool CompleteResultImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;

  std::span<std::byte> buffer_;
  std::chrono::milliseconds timeout_;
  time::TimerId timer_;
};

// Keeps the short-write loop in the caller's coroutine frame. The physical
// write operation is private; callers observe only complete-or-error WriteAll.
class ReactorStream::WriteAllAwaiter final : public OperationHook<ReactorStream::WriteAllAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(WriteAllAwaiter);

  WriteAllAwaiter(ReactorStream& stream, std::span<const std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<void> await_resume() noexcept;

private:
  friend class ReactorStream;
  friend OperationHook<WriteAllAwaiter>;

  void CompleteInline(base::Result<std::size_t> result) noexcept;
  [[nodiscard]] bool CompleteResultImpl(base::Result<std::size_t> result) noexcept;
  [[nodiscard]] bool TryAuthorizeRelease() noexcept;
  [[nodiscard]] bool TryAuthorizeContinuation() noexcept;
  void ScheduleContinuation() noexcept;
  void OnReadyImpl() noexcept;

  ReactorStream* stream_;
  std::span<const std::byte> buffer_;
  SchedulerContinuation continuation_;
  SingleResultLifecycle lifecycle_;
  IoResultState result_;
};

// Owns the destination buffer while a read is pending and returns it on every
// terminal path, so callers cannot invalidate its storage while the backend
// may still access it.
class ReactorStream::ReadIntoAwaiter : public ReadAwaiterState,
                                       public OperationHook<ReactorStream::ReadIntoAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(ReadIntoAwaiter);

  ReadIntoAwaiter(ReactorStream& stream, net::Buffer buffer, std::size_t reserve) noexcept;

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  net::ReadIntoOutcome await_resume() noexcept;

private:
  friend OperationHook<ReadIntoAwaiter>;

  bool CompleteResultImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;
  bool PrepareReservation() noexcept;
  void FinishAttempt(base::Result<std::size_t> result) noexcept;

  net::Buffer buffer_;
  std::size_t reserve_;
  bool reservation_active_{false};
};

}  // namespace coropact::reactor
