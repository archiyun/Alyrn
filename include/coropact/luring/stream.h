// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <linux/time_types.h>
#include <sys/socket.h>

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <span>

#include "coropact/backend/async_stream.h"
#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/op_hook.h"
#include "coropact/luring/detail/op.h"
#include "coropact/net/buffer.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/read_into.h"
#include "coropact/operation/detail/composite_lifecycle.h"
#include "coropact/operation/detail/split_release_lifecycle.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

class LUringLoop;

struct ZeroCopySendResult {
  std::size_t bytes{0};
  bool copied{false};
  // Non-empty REPORT_USAGE sends wait for their F_NOTIF CQE, so this is true
  // whenever a kernel send reached its resource-release boundary.
  bool notification_received{false};
};

namespace detail {

struct ReadSomeForReadTag;
struct ReadSomeForTimeoutTag;

}  // namespace detail

class LUringStream {
public:
  COROPACT_DELETE_COPY(LUringStream);

  class ReadSomeAwaiter;
  class ReadIntoAwaiter;
  class ReadSomeForAwaiter;
  class WriteSomeAwaiter;
  class SendZeroCopyAwaiter;

  LUringStream(LUringLoop* loop, int fd, net::Endpoint peer) noexcept;
  ~LUringStream() noexcept;

  // A stream may move only on its owning loop thread and only while no
  // operation is waiting for a CQE.
  LUringStream(LUringStream&& other) noexcept;
  LUringStream& operator=(LUringStream&& other) noexcept;

  // I/O and lifecycle operations are loop-affine: the coroutine must reach
  // await_suspend() on this stream's owner LUringLoop. Calling them from a
  // different thread violates the runtime contract and terminates through
  // COROPACT_CHECK in every build configuration.
  [[nodiscard]]
  ReadSomeAwaiter ReadSome(std::span<std::byte> buffer) noexcept;

  // Transfers the destination Buffer into the awaiter. The result returns the
  // owner after the CQE has made the kernel's access to its storage terminal.
  [[nodiscard]]
  ReadIntoAwaiter ReadInto(net::Buffer buffer, std::size_t reserve = 4096) noexcept;

  [[nodiscard]]
  ReadSomeForAwaiter ReadSomeFor(std::span<std::byte> buffer,
                                 std::chrono::milliseconds timeout) noexcept;
  [[nodiscard]]
  WriteSomeAwaiter WriteSome(std::span<const std::byte> buffer) noexcept;

  // Optional extension consumed by io::WriteAll. The default remains the
  // ordinary WriteSome() path; enabled streams use SendZeroCopy() and keep
  // the caller's buffer alive until its notification CQE is observed.
  [[nodiscard]]
  bool ZeroCopyWritesEnabled() const noexcept {
    return zero_copy_writes_enabled_;
  }

  void SetZeroCopyWritesEnabled(bool enabled) noexcept { zero_copy_writes_enabled_ = enabled; }

  // Explicit extension API. The await completes only after the send result
  // and, when required, the zero-copy notification have both arrived. This
  // keeps the caller's buffer alive through the kernel's resource lifetime.
  [[nodiscard]]
  SendZeroCopyAwaiter SendZeroCopy(std::span<const std::byte> buffer) noexcept;

  coro::Task<base::Result<void>> Shutdown();
  coro::Task<base::Result<void>> Close();

  [[nodiscard]]
  const net::Endpoint& PeerAddress() const noexcept {
    return peer_;
  }

  [[nodiscard]]
  int Fd() const noexcept {
    return fd_;
  }

  // Native extensions that need to bind an operation to this stream's
  // owning ring use this only while executing on that loop's thread.
  [[nodiscard]]
  LUringLoop* Loop() const noexcept {
    return loop_;
  }

private:
  friend void detail::DispatchStreamCloseComplete(detail::LUringOp* op) noexcept;

  class CloseAwaiter;

  void RequireOwnerLoop() const noexcept;
  void NotifyCloseProgress() noexcept;
  void ResetForMove() noexcept;
  static LUringLoop* PrepareMove(LUringStream& other) noexcept;

  LUringLoop* loop_;
  int fd_{-1};
  net::Endpoint peer_;
  void* pending_read_{nullptr};
  void* pending_write_{nullptr};
  CloseAwaiter* pending_close_{nullptr};
  bool zero_copy_writes_enabled_{false};
  bool closed_{false};
};

class LUringStream::ReadSomeAwaiter : public detail::LUringOpHook<LUringStream::ReadSomeAwaiter> {
public:
  using OpHook = detail::LUringOpHook<ReadSomeAwaiter>;

  COROPACT_DELETE_COPY_MOVE(ReadSomeAwaiter);

  ReadSomeAwaiter(LUringStream& stream, std::span<std::byte> buffer) noexcept
      : OpHook(detail::LUringOpKind::kReadComplete), stream_(&stream), buffer_(buffer) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  base::Result<std::size_t> await_resume() noexcept;

private:
  friend void detail::DispatchStreamReadComplete(detail::LUringOp* op) noexcept;

  static void OnComplete(detail::LUringOp* op) noexcept;

  LUringStream* stream_;
  std::span<std::byte> buffer_;
};

class LUringStream::ReadIntoAwaiter : public detail::LUringOpHook<LUringStream::ReadIntoAwaiter> {
public:
  using OpHook = detail::LUringOpHook<ReadIntoAwaiter>;

  COROPACT_DELETE_COPY_MOVE(ReadIntoAwaiter);

  ReadIntoAwaiter(LUringStream& stream, net::Buffer buffer, std::size_t reserve) noexcept
      : OpHook(detail::LUringOpKind::kReadIntoComplete),
        stream_(&stream),
        buffer_(std::move(buffer)),
        reserve_(reserve) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  net::ReadIntoOutcome await_resume() noexcept;

private:
  friend void detail::DispatchStreamReadIntoComplete(detail::LUringOp* op) noexcept;

  static void OnComplete(detail::LUringOp* op) noexcept;

  [[nodiscard]]
  bool PrepareReservation() noexcept;
  void FinishReservation(base::Result<std::size_t> result) noexcept;

  LUringStream* stream_;
  net::Buffer buffer_;
  std::size_t reserve_;
  std::span<std::byte> writable_;
  bool reservation_active_{false};
};

class LUringStream::ReadSomeForAwaiter
    : public detail::LUringOpHook<LUringStream::ReadSomeForAwaiter, detail::ReadSomeForReadTag>,
      public detail::LUringOpHook<LUringStream::ReadSomeForAwaiter, detail::ReadSomeForTimeoutTag> {
public:
  using ReadOpHook = detail::LUringOpHook<ReadSomeForAwaiter, detail::ReadSomeForReadTag>;
  using TimeoutOpHook = detail::LUringOpHook<ReadSomeForAwaiter, detail::ReadSomeForTimeoutTag>;

  COROPACT_DELETE_COPY_MOVE(ReadSomeForAwaiter);

  ReadSomeForAwaiter(LUringStream& stream, std::span<std::byte> buffer,
                     std::chrono::milliseconds timeout) noexcept;

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  base::Result<std::size_t> await_resume() noexcept;

private:
  friend void detail::DispatchTimedReadComplete(detail::LUringOp* op) noexcept;
  friend void detail::DispatchTimedReadTimeoutComplete(detail::LUringOp* op) noexcept;

  static void OnReadComplete(detail::LUringOp* op) noexcept;
  static void OnTimeoutComplete(detail::LUringOp* op) noexcept;

  detail::LUringOp* ReadOp() noexcept { return ReadOpHook::Op(); }
  detail::LUringOp* TimeoutOp() noexcept { return TimeoutOpHook::Op(); }

  void CompleteRead(detail::LUringOp* current) noexcept;
  void CompleteTimeout(detail::LUringOp* current) noexcept;
  void FinishIfReady(detail::LUringOp* current) noexcept;

  LUringStream* stream_;
  std::span<std::byte> buffer_;
  __kernel_timespec timeout_ts_{};
  std::coroutine_handle<> continuation_{};
  operation::detail::CompositeLifecycle lifecycle_;
};

class LUringStream::WriteSomeAwaiter : public detail::LUringOpHook<LUringStream::WriteSomeAwaiter> {
public:
  using OpHook = detail::LUringOpHook<WriteSomeAwaiter>;

  COROPACT_DELETE_COPY_MOVE(WriteSomeAwaiter);

  WriteSomeAwaiter(LUringStream& stream, std::span<const std::byte> buffer) noexcept
      : OpHook(detail::LUringOpKind::kWriteComplete), stream_(&stream), buffer_(buffer) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  base::Result<std::size_t> await_resume() noexcept;

private:
  friend void detail::DispatchStreamWriteComplete(detail::LUringOp* op) noexcept;

  static void OnComplete(detail::LUringOp* op) noexcept;

  LUringStream* stream_;
  std::span<const std::byte> buffer_;
};

class LUringStream::SendZeroCopyAwaiter
    : public detail::LUringOpHook<LUringStream::SendZeroCopyAwaiter> {
public:
  using OpHook = detail::LUringOpHook<SendZeroCopyAwaiter>;

  COROPACT_DELETE_COPY_MOVE(SendZeroCopyAwaiter);

  SendZeroCopyAwaiter(LUringStream& stream, std::span<const std::byte> buffer) noexcept
      : OpHook(detail::LUringOpKind::kSendZeroCopyComplete), stream_(&stream), buffer_(buffer) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  base::Result<ZeroCopySendResult> await_resume() noexcept;

private:
  friend detail::CompletionDisposition detail::DispatchSendZeroCopyComplete(detail::LUringOp* op,
                                                                    detail::CompletionEvent event) noexcept;

  [[nodiscard]]
  static detail::CompletionDisposition OnComplete(detail::LUringOp* op, detail::CompletionEvent event) noexcept;

  LUringStream* stream_;
  std::span<const std::byte> buffer_;
  operation::detail::SplitReleaseLifecycle lifecycle_;
  bool copied_{false};
  bool notification_received_{false};
};

static_assert(backend::AsyncStream<LUringStream>);
static_assert(backend::AsyncTimedStream<LUringStream>);
static_assert(backend::AsyncOwnedReadStream<LUringStream>);

}  // namespace coropact::luring
