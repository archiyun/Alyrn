// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <sys/uio.h>

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/net/buffer.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/read_into.h"
#include "coropact/net/socket.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
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
  class WriteSomeAwaiter;
  class WriteAllAwaiter;
  class BufferReadAwaiter;
  class BufferWriteAwaiter;

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
  BufferReadAwaiter ReadSome(net::Buffer& buffer, std::size_t reserve = 4096) noexcept;
  [[nodiscard]]
  ReadSomeAwaiter ReadSomeFor(std::span<std::byte> buffer,
                              std::chrono::milliseconds timeout) noexcept;
  [[nodiscard]]
  BufferReadAwaiter ReadSomeFor(net::Buffer& buffer, std::chrono::milliseconds timeout,
                                std::size_t reserve = 4096) noexcept;
  [[nodiscard]]
  WriteSomeAwaiter WriteSome(std::span<const std::byte> buffer) noexcept;
  [[nodiscard]]
  WriteAllAwaiter WriteAll(std::span<const std::byte> buffer) noexcept;
  [[nodiscard]]
  BufferWriteAwaiter WriteSome(net::Buffer& buffer) noexcept;
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
  using CompletionGate = operation::detail::CompletionGate;
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
    kBufferRead,
  };
  enum class PendingWriteKind : std::uint8_t {
    kNone,
    kWriteSome,
    kWriteAll,
    kBufferWrite,
  };

  void* pending_read_{nullptr};
  void* pending_write_{nullptr};
  PendingReadKind pending_read_kind_{PendingReadKind::kNone};
  PendingWriteKind pending_write_kind_{PendingWriteKind::kNone};
  bool closed_{false};
  LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

class ReactorStream::ReadSomeAwaiter final : public OperationHook<ReactorStream::ReadSomeAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(ReadSomeAwaiter);

  ReadSomeAwaiter(ReactorStream& stream, std::span<std::byte> buffer,
                  std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) noexcept
      : stream_(&stream), buffer_(buffer), timeout_(timeout) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  friend OperationHook<ReadSomeAwaiter>;

  void CompleteImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;

  ReactorStream* stream_;
  std::span<std::byte> buffer_;
  std::chrono::milliseconds timeout_;
  SchedulerContinuation continuation_;
  CompletionGate completion_gate_;
  IoResultState result_;
  time::TimerId timer_;
};

class ReactorStream::WriteSomeAwaiter final
    : public OperationHook<ReactorStream::WriteSomeAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(WriteSomeAwaiter);

  WriteSomeAwaiter(ReactorStream& stream, std::span<const std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  friend OperationHook<WriteSomeAwaiter>;

  void CompleteImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;

  ReactorStream* stream_;
  std::span<const std::byte> buffer_;
  SchedulerContinuation continuation_;
  CompletionGate completion_gate_;
  IoResultState result_;
};

// Keeps the write loop in the caller's coroutine frame. ReactorStream uses
// this native extension so a buffered response does not allocate a nested
// Task frame just to repeat WriteSome until the span is drained.
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
  friend OperationHook<WriteAllAwaiter>;

  void CompleteImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;

  ReactorStream* stream_;
  std::span<const std::byte> buffer_;
  SchedulerContinuation continuation_;
  CompletionGate completion_gate_;
  IoResultState result_;
};

class ReactorStream::BufferReadAwaiter : public OperationHook<ReactorStream::BufferReadAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(BufferReadAwaiter);

  BufferReadAwaiter(ReactorStream& stream, net::Buffer& buffer, std::size_t reserve,
                    std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) noexcept;

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  friend OperationHook<BufferReadAwaiter>;

  void CompleteImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;
  bool PrepareReservation() noexcept;
  void FinishAttempt(base::Result<std::size_t> result) noexcept;

  ReactorStream* stream_;
  net::Buffer* buffer_;
  std::size_t reserve_;
  std::chrono::milliseconds timeout_;
  std::vector<iovec> iovs_;
  SchedulerContinuation continuation_;
  CompletionGate completion_gate_;
  IoResultState result_;
  time::TimerId timer_;
};

// Owns the destination buffer while a read is pending. Unlike ReadSome(Buffer&),
// this awaiter returns the Buffer on every terminal path, so callers cannot
// invalidate its storage while the backend may still access it.
class ReactorStream::ReadIntoAwaiter : public OperationHook<ReactorStream::ReadIntoAwaiter> {
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

  void CompleteImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;
  bool PrepareReservation() noexcept;
  void FinishAttempt(base::Result<std::size_t> result) noexcept;

  ReactorStream* stream_;
  net::Buffer buffer_;
  std::size_t reserve_;
  std::vector<iovec> iovs_;
  SchedulerContinuation continuation_;
  CompletionGate completion_gate_;
  IoResultState result_;
  bool reservation_active_{false};
};

class ReactorStream::BufferWriteAwaiter : public OperationHook<ReactorStream::BufferWriteAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(BufferWriteAwaiter);

  BufferWriteAwaiter(ReactorStream& stream, net::Buffer& buffer) noexcept;

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  friend OperationHook<BufferWriteAwaiter>;

  void CompleteImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;
  bool PrepareReadable() noexcept;
  void FinishAttempt(base::Result<std::size_t> result) noexcept;

  ReactorStream* stream_;
  net::Buffer* buffer_;
  std::vector<iovec> iovs_;
  SchedulerContinuation continuation_;
  CompletionGate completion_gate_;
  IoResultState result_;
};

}  // namespace coropact::reactor
