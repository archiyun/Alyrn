// SPDX-License-Identifier: MIT
#pragma once

#include <sys/uio.h>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <span>

#include "alyrn/backend/async_stream.h"
#include "alyrn/kqueue/detail/channel.h"
#include "alyrn/kqueue/detail/loop_shutdown.h"
#include "alyrn/kqueue/detail/op_hook.h"
#include "alyrn/kqueue/detail/result_state.h"
#include "alyrn/kqueue/loop.h"
#include "alyrn/kqueue/options.h"
#include "alyrn/net/buffer.h"
#include "alyrn/net/detail/stream_lifecycle.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/read_into.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/detail/scheduler_continuation.h"
#include "alyrn/detail/single_result_lifecycle.h"
#include "alyrn/result.h"
#include "alyrn/task.h"
#include "alyrn/time/clock.h"
#include "alyrn/detail/macros.h"

namespace alyrn::kqueue {

/*
 * One-shot readiness is the only supported stream mode for now: each await arms
 * a filter, delivery retires it, and the next await must re-arm.
 */
struct StreamOptions {
  TriggerMode trigger_mode{TriggerMode::kOneShot};
};

class Stream {
private:
  class ReadSomeAwaiter;
  class ReadIntoAwaiter;
  class ReadAwaiterState;
  class WriteAllAwaiter;

public:
  ALYRN_DELETE_COPY(Stream);

  Stream(Loop* loop, int fd, net::Endpoint peer = net::Endpoint(0), StreamOptions options = {});
  ~Stream();

  Stream(Stream&& other) noexcept;
  Stream& operator=(Stream&& other) noexcept;

  ReadSomeAwaiter ReadSome(std::span<std::byte> buffer) noexcept;
  ReadIntoAwaiter ReadInto(net::Buffer buffer, std::size_t reserve = 4096) noexcept;
  WriteAllAwaiter WriteAll(std::span<const std::byte> buffer) noexcept;

  // Legacy alias for CloseWrite().
  Task<Result<void>> Shutdown() noexcept;
  Task<Result<void>> Close() noexcept;

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

  // Shuts down local reception while keeping the descriptor and write side.
  Task<Result<void>> CloseRead() noexcept;

  // Shuts down local transmission while keeping the descriptor and read side.
  Task<Result<void>> CloseWrite() noexcept;

  [[nodiscard]]
  int Fd() const noexcept {
    return socket_.fd();
  }

  [[nodiscard]]
  Loop* OwnerLoop() const noexcept {
    return loop_;
  }

  // Detaches this stream from its loop and returns the descriptor. The
  // caller must reconstruct a Stream on the destination loop; this is
  // the master-slave handoff seam and is owner-thread-only.
  [[nodiscard]]
  int Release() noexcept;

private:
  using Channel = detail::Channel;
  using LoopShutdownParticipant = detail::LoopShutdownParticipant;
  using SchedulerContinuation = ::alyrn::detail::SchedulerContinuation;
  using SingleResultLifecycle = ::alyrn::detail::SingleResultLifecycle;
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

class Stream::ReadAwaiterState {
public:
  bool TryAuthorizeRelease() noexcept;
  bool TryAuthorizeContinuation() noexcept;
  void ScheduleContinuation() noexcept;

protected:
  explicit ReadAwaiterState(Stream& stream) noexcept : stream_(&stream) {}

  bool BeginRead(std::coroutine_handle<> continuation) noexcept;
  void SuspendForRead(void* awaiter, PendingReadKind kind) noexcept;

  bool TryAuthorizeResult() noexcept;
  void CompleteInline(Result<std::size_t> result) noexcept;
  void CompleteStoredInline() noexcept;
  void SetResult(Result<std::size_t> result) noexcept;

  Result<std::size_t> TakeResult() noexcept;

  Stream* stream_;
  SchedulerContinuation continuation_;
  SingleResultLifecycle lifecycle_;
  IoResultState result_;
};

class [[nodiscard]] Stream::ReadSomeAwaiter final : public ReadAwaiterState,
                                      public OperationHook<Stream::ReadSomeAwaiter> {
public:
  ALYRN_DELETE_COPY_MOVE(ReadSomeAwaiter);

  explicit ReadSomeAwaiter(Stream& stream, std::span<std::byte> buffer) noexcept
      : ReadAwaiterState(stream), buffer_(buffer) {}

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<std::size_t> await_resume() noexcept;

private:
  friend OperationHook<ReadSomeAwaiter>;

  bool CompleteResultImpl(Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;

  std::span<std::byte> buffer_;
};

class [[nodiscard]] Stream::WriteAllAwaiter final : public OperationHook<Stream::WriteAllAwaiter> {
public:
  ALYRN_DELETE_COPY_MOVE(WriteAllAwaiter);

  WriteAllAwaiter(Stream& stream, std::span<const std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<void> await_resume() noexcept;

private:
  friend class Stream;
  friend OperationHook<WriteAllAwaiter>;

  void CompleteInline(Result<std::size_t> result) noexcept;
  bool CompleteResultImpl(Result<std::size_t> result) noexcept;
  bool TryAuthorizeRelease() noexcept;
  bool TryAuthorizeContinuation() noexcept;
  void ScheduleContinuation() noexcept;
  void OnReadyImpl() noexcept;

  Stream* stream_;
  std::span<const std::byte> buffer_;
  SchedulerContinuation continuation_;
  SingleResultLifecycle lifecycle_;
  IoResultState result_;
};

class [[nodiscard]] Stream::ReadIntoAwaiter : public ReadAwaiterState,
                                public OperationHook<Stream::ReadIntoAwaiter> {
public:
  ALYRN_DELETE_COPY_MOVE(ReadIntoAwaiter);

  ReadIntoAwaiter(Stream& stream, net::Buffer buffer, std::size_t reserve) noexcept;

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
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

static_assert(backend::AsyncStream<Stream>);
static_assert(backend::AsyncReadIntoStream<Stream>);

}  // namespace alyrn::kqueue
