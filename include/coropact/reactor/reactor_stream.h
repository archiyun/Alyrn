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
#include "coropact/net/endpoint.h"
#include "coropact/net/segmented_buffer.h"
#include "coropact/net/socket.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/reactor/channel.h"
#include "coropact/reactor/detail/op_hook.h"
#include "coropact/reactor/detail/result_state.h"
#include "coropact/reactor/event_loop.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

class ReactorStream {
public:
  COROPACT_DELETE_COPY(ReactorStream);

  ReactorStream(EventLoop* loop, int fd, net::Endpoint peer = net::Endpoint(0));
  ~ReactorStream();

  // Moves are loop-affine: the source must be used from its owning loop
  // thread and must not have a pending read or write operation.
  ReactorStream(ReactorStream&& other) noexcept;
  ReactorStream& operator=(ReactorStream&& other) noexcept;

  class ReadSomeAwaiter;
  class WriteSomeAwaiter;
  class BufferReadAwaiter;
  class BufferWriteAwaiter;

  [[nodiscard]]
  ReadSomeAwaiter ReadSome(std::span<std::byte> buffer) noexcept;
  [[nodiscard]]
  BufferReadAwaiter ReadSome(net::SegmentedBuffer& buffer,
                             std::size_t reserve = 4096) noexcept;
  [[nodiscard]]
  ReadSomeAwaiter ReadSomeFor(std::span<std::byte> buffer,
                              std::chrono::milliseconds timeout) noexcept;
  [[nodiscard]]
  BufferReadAwaiter ReadSomeFor(net::SegmentedBuffer& buffer,
                                std::chrono::milliseconds timeout,
                                std::size_t reserve = 4096) noexcept;
  [[nodiscard]]
  WriteSomeAwaiter WriteSome(std::span<const std::byte> buffer) noexcept;
  [[nodiscard]]
  BufferWriteAwaiter WriteSome(net::SegmentedBuffer& buffer) noexcept;
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
  void HandleRead(time::Timestamp receive_time);
  void HandleWrite();
  void HandleClose();
  void HandleError();

  static void DispatchRead(void* context, time::Timestamp receive_time) noexcept;
  static void DispatchWrite(void* context) noexcept;
  static void DispatchClose(void* context) noexcept;
  static void DispatchError(void* context) noexcept;

  void CompleteRead(base::Result<std::size_t> result);
  void CompleteWrite(base::Result<std::size_t> result);
  void DetachChannel();
  void RequireOwnerLoop() const noexcept;
  void BindChannelCallbacks() noexcept;
  void ResetForMove() noexcept;
  static EventLoop* PrepareMove(ReactorStream& other) noexcept;

  EventLoop* loop_;
  net::Socket socket_;
  Channel channel_;
  net::Endpoint peer_;
  enum class PendingReadKind : std::uint8_t {
    kNone,
    kReadSome,
    kBufferRead,
  };
  enum class PendingWriteKind : std::uint8_t {
    kNone,
    kWriteSome,
    kBufferWrite,
  };

  void* pending_read_{nullptr};
  void* pending_write_{nullptr};
  PendingReadKind pending_read_kind_{PendingReadKind::kNone};
  PendingWriteKind pending_write_kind_{PendingWriteKind::kNone};
  bool closed_{false};
};

class ReactorStream::ReadSomeAwaiter final
    : public detail::ReactorOperationHook<ReactorStream::ReadSomeAwaiter> {
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
  friend class detail::ReactorOperationHook<ReadSomeAwaiter>;

  void CompleteImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;

  ReactorStream* stream_;
  std::span<std::byte> buffer_;
  std::chrono::milliseconds timeout_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  detail::ReactorIoResultState result_;
  time::TimerId timer_;
};

class ReactorStream::WriteSomeAwaiter final
    : public detail::ReactorOperationHook<ReactorStream::WriteSomeAwaiter> {
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
  friend class detail::ReactorOperationHook<WriteSomeAwaiter>;

  void CompleteImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;

  ReactorStream* stream_;
  std::span<const std::byte> buffer_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  detail::ReactorIoResultState result_;
};

class ReactorStream::BufferReadAwaiter
    : public detail::ReactorOperationHook<ReactorStream::BufferReadAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(BufferReadAwaiter);

  BufferReadAwaiter(ReactorStream& stream, net::SegmentedBuffer& buffer, std::size_t reserve,
                    std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) noexcept;

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  friend class detail::ReactorOperationHook<BufferReadAwaiter>;

  void CompleteImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;
  bool PrepareReservation() noexcept;
  void FinishAttempt(base::Result<std::size_t> result) noexcept;

  ReactorStream* stream_;
  net::SegmentedBuffer* buffer_;
  std::size_t reserve_;
  std::chrono::milliseconds timeout_;
  std::vector<iovec> iovs_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  detail::ReactorIoResultState result_;
  time::TimerId timer_;
};

class ReactorStream::BufferWriteAwaiter
    : public detail::ReactorOperationHook<ReactorStream::BufferWriteAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(BufferWriteAwaiter);

  BufferWriteAwaiter(ReactorStream& stream, net::SegmentedBuffer& buffer) noexcept;

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  friend class detail::ReactorOperationHook<BufferWriteAwaiter>;

  void CompleteImpl(base::Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;
  bool PrepareReadable() noexcept;
  void FinishAttempt(base::Result<std::size_t> result) noexcept;

  ReactorStream* stream_;
  net::SegmentedBuffer* buffer_;
  std::vector<iovec> iovs_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  detail::ReactorIoResultState result_;
};

}  // namespace coropact::reactor
