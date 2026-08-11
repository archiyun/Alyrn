// SPDX-License-Identifier: MIT
#pragma once

#include <linux/time_types.h>
#include <sys/socket.h>
#include <sys/uio.h>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "coropact/backend/async_stream.h"
#include "coropact/result.h"
#include "coropact/coro/task.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/detail/op_hook.h"
#include "coropact/net/buffer.h"
#include "coropact/net/detail/stream_lifecycle.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/read_into.h"
#include "coropact/operation/detail/composite_lifecycle.h"
#include "coropact/operation/detail/split_release_lifecycle.h"
#include "coropact/time/clock.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

class LUringLoop;

enum class ZeroCopySendUsage : std::uint8_t {
  // The primary CQE was terminal, so the kernel emitted no usage report.
  // Do not infer a copy or zero-copy outcome from this value.
  kUnknown,
  kZeroCopy,
  kCopied,
};

struct ZeroCopySendResult {
  std::size_t bytes{0};
  ZeroCopySendUsage usage{ZeroCopySendUsage::kUnknown};
  // True only when the primary CQE advertised a later F_NOTIF CQE and that
  // notification was observed. A primary CQE without F_MORE is itself the
  // resource-release boundary.
  bool notification_received{false};
};

namespace detail {

struct ReadSomeForReadTag;
struct ReadSomeForTimeoutTag;
class StreamOperationSlot;

}  // namespace detail

class LUringStream {
private:
  class ReadSomeAwaiter;
  class ReadIntoAwaiter;
  class ReadSomeForAwaiter;
  class SendAwaiter;
  class SendZeroCopyAwaiter;

public:
  COROPACT_DELETE_COPY(LUringStream);

  // Read/write methods intentionally return private awaiter types. Call them
  // directly with co_await (or keep the result in auto); SQE/CQE ownership is
  // implementation detail, not a stream interface.

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
  ReadSomeForAwaiter ReadSomeFor(std::span<std::byte> buffer, time::Duration timeout) noexcept;
  [[nodiscard]]
  coro::Task<Result<void>> WriteAll(std::span<const std::byte> buffer);

  // WriteAll() chooses this optional extension when enabled. SendZeroCopy()
  // keeps the caller's buffer alive until its notification CQE is observed.
  [[nodiscard]]
  bool ZeroCopyWritesEnabled() const noexcept {
    return zero_copy_writes_enabled_;
  }

  void SetZeroCopyWritesEnabled(bool enabled) noexcept { zero_copy_writes_enabled_ = enabled; }

  // Explicit extension API. The await completes after the send result and
  // after the kernel's resource-release boundary: either the primary CQE
  // itself, or the F_NOTIF CQE promised by primary F_MORE. This keeps the
  // caller's buffer alive through the kernel's resource lifetime.
  [[nodiscard]]
  SendZeroCopyAwaiter SendZeroCopy(std::span<const std::byte> buffer) noexcept;

  coro::Task<Result<void>> Shutdown();
  coro::Task<Result<void>> Close();

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
  friend void detail::DispatchStreamReadComplete(detail::LUringOp* op) noexcept;
  friend void detail::DispatchStreamReadIntoComplete(detail::LUringOp* op) noexcept;
  friend void detail::DispatchTimedReadComplete(detail::LUringOp* op) noexcept;
  friend void detail::DispatchTimedReadTimeoutComplete(detail::LUringOp* op) noexcept;
  friend void detail::DispatchStreamWriteComplete(detail::LUringOp* op) noexcept;
  friend detail::CompletionDisposition detail::DispatchSendZeroCopyComplete(
      detail::LUringOp* op, detail::CompletionEvent event) noexcept;
  friend void detail::DispatchStreamCloseComplete(detail::LUringOp* op) noexcept;
  friend class detail::StreamOperationSlot;

  class CloseAwaiter;

  [[nodiscard]]
  SendAwaiter Send(std::span<const std::byte> buffer) noexcept;

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
  net::detail::StreamLifecycle lifecycle_;
};

// --- ReadSomeAwaiter ---
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

  Result<std::size_t> await_resume() noexcept;

private:
  friend void detail::DispatchStreamReadComplete(detail::LUringOp* op) noexcept;

  static void OnComplete(detail::LUringOp* op) noexcept;

  LUringStream* stream_;
  std::span<std::byte> buffer_;
};

// --- ReadIntoAwaiter ---
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

  enum class ReservationKind : std::uint8_t {
    kNone,
    kSingle,
    kMultiple,
  };

  [[nodiscard]]
  ReservationKind PrepareReservation(iovec& single_iov) noexcept;
  void FinishReservation(Result<std::size_t> result) noexcept;

  LUringStream* stream_;
  net::Buffer buffer_;
  std::size_t reserve_;
  // READV SQEs retain their iovec array until a terminal CQE. A single-range
  // RECV copies its base and length into the SQE, so its local iovec does not
  // need to survive await_suspend().
  std::vector<iovec> iovs_;
  ReservationKind reservation_kind_{ReservationKind::kNone};
};

// --- ReadSomeForAwaiter ---
class LUringStream::ReadSomeForAwaiter
    : public detail::LUringOpHook<LUringStream::ReadSomeForAwaiter, detail::ReadSomeForReadTag>,
      public detail::LUringOpHook<LUringStream::ReadSomeForAwaiter, detail::ReadSomeForTimeoutTag> {
public:
  using ReadOpHook = detail::LUringOpHook<ReadSomeForAwaiter, detail::ReadSomeForReadTag>;
  using TimeoutOpHook = detail::LUringOpHook<ReadSomeForAwaiter, detail::ReadSomeForTimeoutTag>;

  COROPACT_DELETE_COPY_MOVE(ReadSomeForAwaiter);

  ReadSomeForAwaiter(LUringStream& stream, std::span<std::byte> buffer,
                     time::Duration timeout) noexcept;

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  Result<std::size_t> await_resume() noexcept;

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

class LUringStream::SendAwaiter : public detail::LUringOpHook<LUringStream::SendAwaiter> {
public:
  using OpHook = detail::LUringOpHook<SendAwaiter>;

  COROPACT_DELETE_COPY_MOVE(SendAwaiter);

  SendAwaiter(LUringStream& stream, std::span<const std::byte> buffer) noexcept
      : OpHook(detail::LUringOpKind::kWriteComplete), stream_(&stream), buffer_(buffer) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  Result<std::size_t> await_resume() noexcept;

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

  Result<ZeroCopySendResult> await_resume() noexcept;

private:
  friend detail::CompletionDisposition detail::DispatchSendZeroCopyComplete(
      detail::LUringOp* op, detail::CompletionEvent event) noexcept;

  [[nodiscard]]
  static detail::CompletionDisposition OnComplete(detail::LUringOp* op,
                                                  detail::CompletionEvent event) noexcept;

  LUringStream* stream_;
  std::span<const std::byte> buffer_;
  operation::detail::SplitReleaseLifecycle lifecycle_;
  ZeroCopySendUsage usage_{ZeroCopySendUsage::kUnknown};
  bool notification_received_{false};
};

static_assert(backend::AsyncStream<LUringStream>);
static_assert(backend::AsyncTimedStream<LUringStream>);
static_assert(backend::AsyncReadIntoStream<LUringStream>);

}  // namespace coropact::luring
