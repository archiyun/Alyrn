// SPDX-License-Identifier: MIT
#pragma once

#include <sys/socket.h>
#include <sys/uio.h>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "alyrn/backend/async_stream.h"
#include "alyrn/detail/macros.h"
#include "alyrn/detail/split_release_lifecycle.h"
#include "alyrn/net/buffer.h"
#include "alyrn/net/detail/stream_lifecycle.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/recv.h"
#include "alyrn/result.h"
#include "alyrn/task.h"
#include "alyrn/time/clock.h"
#include "alyrn/time/timer_id.h"
#include "alyrn/uring/detail/completion_dispatch.h"
#include "alyrn/uring/detail/op.h"
#include "alyrn/uring/detail/op_hook.h"

namespace alyrn::uring {

class Loop;

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

class ProvidedBufferPool;
class StreamOperationSlot;

}  // namespace detail

class Stream {
private:
  class ReadAwaiter;
  class RecvAwaiter;
  class RecvCopyAwaiter;
  class SendAwaiter;
  class SendZeroCopyAwaiter;

public:
  ALYRN_DELETE_COPY(Stream);

  // Read/write methods intentionally return private awaiter types. Call them
  // directly with co_await (or keep the result in auto); SQE/CQE ownership is
  // implementation detail, not a stream interface.

  Stream(Loop* loop, int fd, net::Endpoint peer) noexcept;
  ~Stream() noexcept;

  // A stream may move only on its owning loop thread and only while no
  // operation is waiting for a CQE.
  Stream(Stream&& other) noexcept;
  Stream& operator=(Stream&& other) noexcept;

  // I/O and lifecycle operations are loop-affine: the coroutine must reach
  // await_suspend() on this stream's owner Loop. Calling them from a
  // different thread violates the runtime contract and terminates through
  // ALYRN_CHECK in every build configuration.
  ReadAwaiter Read(std::span<std::byte> buffer) noexcept;

  // Fills a kernel provided-buffer slot, copies into a caller-owned Buffer, and
  // returns the slot to the shared ring before resume.
  RecvCopyAwaiter Recv() noexcept;

  // Transfers the destination Buffer into the awaiter. The result returns the
  // owner after the CQE has made the kernel's access to its storage terminal.
  RecvAwaiter Recv(net::Buffer buffer, std::size_t reserve = 4096) noexcept;

  Task<Result<void>> Write(std::span<const std::byte> buffer);

  // Write() chooses this optional extension when enabled. SendZeroCopy()
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
  SendZeroCopyAwaiter SendZeroCopy(std::span<const std::byte> buffer) noexcept;

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

  // Sticky monotonic deadlines. They apply to the current pending operation
  // and all later operations in the selected direction. nullopt clears a
  // deadline. These setters are loop-affine, like stream I/O.
  //
  // Once the owning loop observes expiry, that operation reports ETIMEDOUT
  // even if a competing I/O CQE also reaches the loop. A pending operation is
  // canceled through its own io_uring user_data and resumes only after both
  // the I/O CQE and cancellation CQE have reached the loop.
  [[nodiscard]]
  Result<void> SetDeadline(std::optional<time::Deadline> deadline) noexcept;

  [[nodiscard]]
  Result<void> SetReadDeadline(std::optional<time::Deadline> deadline) noexcept;

  [[nodiscard]]
  Result<void> SetWriteDeadline(std::optional<time::Deadline> deadline) noexcept;

  // Shuts down local reception while keeping the descriptor and write side.
  Task<Result<void>> CloseRead() noexcept;

  // Shuts down local transmission while keeping the descriptor and read side.
  Task<Result<void>> CloseWrite() noexcept;

  [[nodiscard]]
  int Fd() const noexcept {
    return fd_;
  }

  // Native extensions that need to bind an operation to this stream's
  // owning ring use this only while executing on that loop's thread.
  [[nodiscard]]
  Loop* OwnerLoop() const noexcept {
    return loop_;
  }

private:
  friend void detail::DispatchStreamReadComplete(detail::Op* op) noexcept;
  friend void detail::DispatchStreamRecvComplete(detail::Op* op) noexcept;
  friend void detail::DispatchStreamRecvCopyComplete(detail::Op* op,
                                                     detail::CompletionEvent event) noexcept;
  friend void detail::DispatchStreamWriteComplete(detail::Op* op) noexcept;
  friend detail::CompletionDisposition detail::DispatchSendZeroCopyComplete(
      detail::Op* op, detail::CompletionEvent event) noexcept;
  friend void detail::DispatchStreamCloseComplete(detail::Op* op) noexcept;
  friend detail::CompletionDisposition detail::DispatchStreamReadCancelComplete(
      detail::Op* op) noexcept;
  friend detail::CompletionDisposition detail::DispatchStreamWriteCancelComplete(
      detail::Op* op) noexcept;
  friend class detail::StreamOperationSlot;

  class CloseAwaiter;

  SendAwaiter Send(std::span<const std::byte> buffer) noexcept;

  void RequireOwnerLoop() const noexcept;
  void NotifyCloseProgress() noexcept;
  void ResetForMove() noexcept;
  static Loop* PrepareMove(Stream& other) noexcept;

  using DeadlineFinalizer = void (*)(void*) noexcept;

  class DeadlineCancelOp : public detail::OpHook<DeadlineCancelOp> {
  public:
    DeadlineCancelOp(Stream& stream, bool read) noexcept
        : OpHook(read ? detail::OpKind::kStreamReadCancelComplete
                      : detail::OpKind::kStreamWriteCancelComplete),
          stream_(&stream),
          read_(read) {}

    static detail::CompletionDisposition OnComplete(detail::Op* op) noexcept;

  private:
    Stream* stream_;
    bool read_;
  };

  Result<void> BeginReadOperation(detail::Op* operation, void* owner,
                                  DeadlineFinalizer finalizer) noexcept;
  Result<void> BeginWriteOperation(detail::Op* operation, void* owner,
                                   DeadlineFinalizer finalizer) noexcept;
  void AbortReadOperation(detail::Op* operation) noexcept;
  void AbortWriteOperation(detail::Op* operation) noexcept;
  void CompleteReadOperation(detail::Op* operation) noexcept;
  void CompleteWriteOperation(detail::Op* operation) noexcept;
  detail::CompletionDisposition CompleteDeadlineCancel(bool read) noexcept;
  bool ReadDeadlineTimedOut() const noexcept { return read_timed_out_; }
  bool WriteDeadlineTimedOut() const noexcept { return write_timed_out_; }
  void HandleReadDeadline(std::uint64_t generation) noexcept;
  void HandleWriteDeadline(std::uint64_t generation) noexcept;
  void RetryReadDeadlineCancel(std::uint64_t generation) noexcept;
  void RetryWriteDeadlineCancel(std::uint64_t generation) noexcept;
  Result<void> SubmitReadDeadlineCancel() noexcept;
  Result<void> SubmitWriteDeadlineCancel() noexcept;
  bool ArmReadDeadlineTimer() noexcept;
  bool ArmWriteDeadlineTimer() noexcept;
  void CancelReadDeadlineTimer() noexcept;
  void CancelWriteDeadlineTimer() noexcept;
  bool ScheduleReadDeadlineCancelRetry(std::uint64_t generation) noexcept;
  bool ScheduleWriteDeadlineCancelRetry(std::uint64_t generation) noexcept;
  void FinalizeReadOperation() noexcept;
  void FinalizeWriteOperation() noexcept;

  Loop* loop_;
  int fd_{-1};
  net::Endpoint peer_;
  void* pending_read_{nullptr};
  void* pending_write_{nullptr};
  CloseAwaiter* pending_close_{nullptr};
  bool zero_copy_writes_enabled_{false};
  net::detail::StreamLifecycle lifecycle_;
  std::optional<time::Deadline> read_deadline_;
  std::optional<time::Deadline> write_deadline_;
  time::TimerId read_timer_;
  time::TimerId write_timer_;
  std::uint64_t read_timer_generation_{0};
  std::uint64_t write_timer_generation_{0};
  DeadlineCancelOp read_cancel_op_;
  DeadlineCancelOp write_cancel_op_;
  detail::Op* read_deadline_target_{nullptr};
  detail::Op* write_deadline_target_{nullptr};
  void* read_deadline_owner_{nullptr};
  void* write_deadline_owner_{nullptr};
  DeadlineFinalizer read_deadline_finalizer_{nullptr};
  DeadlineFinalizer write_deadline_finalizer_{nullptr};
  bool read_cancel_requested_{false};
  bool write_cancel_requested_{false};
  bool read_cancel_submitted_{false};
  bool write_cancel_submitted_{false};
  bool read_cancel_terminal_{false};
  bool write_cancel_terminal_{false};
  bool read_primary_terminal_{false};
  bool write_primary_terminal_{false};
  bool read_timed_out_{false};
  bool write_timed_out_{false};
};

// --- ReadAwaiter ---
class [[nodiscard]] Stream::ReadAwaiter : public detail::OpHook<Stream::ReadAwaiter> {
public:
  using OpHook = detail::OpHook<ReadAwaiter>;

  ALYRN_DELETE_COPY_MOVE(ReadAwaiter);

  ReadAwaiter(Stream& stream, std::span<std::byte> buffer) noexcept
      : OpHook(detail::OpKind::kReadComplete), stream_(&stream), buffer_(buffer) {}

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<std::size_t> await_resume() noexcept;

private:
  friend void detail::DispatchStreamReadComplete(detail::Op* op) noexcept;

  static void OnComplete(detail::Op* op) noexcept;
  static void Finalize(void* owner) noexcept;

  Stream* stream_;
  std::span<std::byte> buffer_;
  bool timed_out_{false};
};

// --- RecvAwaiter ---
class [[nodiscard]] Stream::RecvAwaiter : public detail::OpHook<Stream::RecvAwaiter> {
public:
  using OpHook = detail::OpHook<RecvAwaiter>;

  ALYRN_DELETE_COPY_MOVE(RecvAwaiter);

  RecvAwaiter(Stream& stream, net::Buffer buffer, std::size_t reserve) noexcept
      : OpHook(detail::OpKind::kRecvComplete),
        stream_(&stream),
        buffer_(std::move(buffer)),
        reserve_(reserve) {}

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  net::RecvOutcome await_resume() noexcept;

private:
  friend void detail::DispatchStreamRecvComplete(detail::Op* op) noexcept;

  static void OnComplete(detail::Op* op) noexcept;
  static void Finalize(void* owner) noexcept;

  enum class ReservationKind : std::uint8_t {
    kNone,
    kSingle,
    kMultiple,
  };

  ReservationKind PrepareReservation(iovec& single_iov) noexcept;
  void FinishReservation(Result<std::size_t> result) noexcept;

  Stream* stream_;
  net::Buffer buffer_;
  std::size_t reserve_;
  // READV SQEs retain their iovec array until a terminal CQE. A single-range
  // RECV copies its base and length into the SQE, so its local iovec does not
  // need to survive await_suspend().
  std::vector<iovec> iovs_;
  ReservationKind reservation_kind_{ReservationKind::kNone};
  bool timed_out_{false};
};

class [[nodiscard]] Stream::RecvCopyAwaiter : public detail::OpHook<Stream::RecvCopyAwaiter> {
public:
  using OpHook = detail::OpHook<RecvCopyAwaiter>;

  ALYRN_DELETE_COPY_MOVE(RecvCopyAwaiter);

  explicit RecvCopyAwaiter(Stream& stream) noexcept
      : OpHook(detail::OpKind::kRecvCopyComplete), stream_(&stream) {}

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<net::Buffer> await_resume() noexcept;

private:
  friend void detail::DispatchStreamRecvCopyComplete(detail::Op* op,
                                                     detail::CompletionEvent event) noexcept;

  static void OnComplete(detail::Op* op, detail::CompletionEvent event) noexcept;
  static void Finalize(void* owner) noexcept;
  void Finish(Result<net::Buffer> outcome) noexcept;

  Stream* stream_;
  detail::ProvidedBufferPool* pool_{nullptr};
  std::optional<Result<net::Buffer>> outcome_;
  bool timed_out_{false};
};

class [[nodiscard]] Stream::SendAwaiter : public detail::OpHook<Stream::SendAwaiter> {
public:
  using OpHook = detail::OpHook<SendAwaiter>;

  ALYRN_DELETE_COPY_MOVE(SendAwaiter);

  SendAwaiter(Stream& stream, std::span<const std::byte> buffer) noexcept
      : OpHook(detail::OpKind::kWriteComplete), stream_(&stream), buffer_(buffer) {}

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<std::size_t> await_resume() noexcept;

private:
  friend void detail::DispatchStreamWriteComplete(detail::Op* op) noexcept;

  static void OnComplete(detail::Op* op) noexcept;
  static void Finalize(void* owner) noexcept;

  Stream* stream_;
  std::span<const std::byte> buffer_;
  bool timed_out_{false};
};

class [[nodiscard]] Stream::SendZeroCopyAwaiter
    : public detail::OpHook<Stream::SendZeroCopyAwaiter> {
public:
  using OpHook = detail::OpHook<SendZeroCopyAwaiter>;

  ALYRN_DELETE_COPY_MOVE(SendZeroCopyAwaiter);

  SendZeroCopyAwaiter(Stream& stream, std::span<const std::byte> buffer) noexcept
      : OpHook(detail::OpKind::kSendZeroCopyComplete), stream_(&stream), buffer_(buffer) {}

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<ZeroCopySendResult> await_resume() noexcept;

private:
  friend detail::CompletionDisposition detail::DispatchSendZeroCopyComplete(
      detail::Op* op, detail::CompletionEvent event) noexcept;

  static detail::CompletionDisposition OnComplete(detail::Op* op,
                                                  detail::CompletionEvent event) noexcept;
  static void Finalize(void* owner) noexcept;

  Stream* stream_;
  std::span<const std::byte> buffer_;
  ::alyrn::detail::SplitReleaseLifecycle lifecycle_;
  ZeroCopySendUsage usage_{ZeroCopySendUsage::kUnknown};
  bool notification_received_{false};
  bool timed_out_{false};
};

static_assert(backend::AsyncStream<Stream>);
static_assert(backend::AsyncDeadlineStream<Stream>);
static_assert(backend::AsyncRecvStream<Stream>);
static_assert(backend::AsyncRecvCopyStream<Stream>);

}  // namespace alyrn::uring
