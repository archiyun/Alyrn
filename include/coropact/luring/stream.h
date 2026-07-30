// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <linux/time_types.h>
#include <sys/socket.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <span>

#include "coropact/backend/async_stream.h"
#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/op_hook.h"
#include "coropact/luring/op.h"
#include "coropact/net/endpoint.h"
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

// Diagnostic classification for the optional send-zero-copy extension. The
// ordinary stream API still returns base::Error; these counters preserve the
// boundary at which a failure was observed without changing every Result.
enum class ZeroCopySendErrorKind : std::uint8_t {
  kClosed,
  kProfileUnavailable,
  kBusy,
  kSubmission,
  kProtocol,
};

struct ZeroCopySendDiagnostics {
  std::atomic<std::uint64_t> attempts{0};
  std::atomic<std::uint64_t> logical_completions{0};
  std::atomic<std::uint64_t> primary_events{0};
  std::atomic<std::uint64_t> notification_events{0};
  std::atomic<std::uint64_t> copied_completions{0};
  std::atomic<std::uint64_t> copy_fallbacks{0};
  // Setup and protocol failures. Primary CQE failures are recorded below so
  // an ENOMEM that io::WriteAll recovered by copy fallback is not reported as
  // a terminal write error.
  std::atomic<std::uint64_t> errors{0};
  std::atomic<std::uint64_t> closed_errors{0};
  std::atomic<std::uint64_t> profile_errors{0};
  std::atomic<std::uint64_t> busy_errors{0};
  std::atomic<std::uint64_t> submission_errors{0};
  // Raw first-CQE failures, independently classified even when a higher-level
  // write algorithm safely falls back to a regular send.
  std::atomic<std::uint64_t> primary_errors{0};
  std::atomic<std::uint64_t> primary_enomem_errors{0};
  std::atomic<std::uint64_t> primary_epipe_errors{0};
  std::atomic<std::uint64_t> primary_connection_reset_errors{0};
  std::atomic<std::uint64_t> primary_cancelled_errors{0};
  std::atomic<std::uint64_t> primary_other_errors{0};
  std::atomic<std::uint64_t> protocol_errors{0};
  std::atomic<int> last_primary_result{0};
  std::atomic<int> last_notification_result{0};
  std::atomic<int> first_primary_error{0};
  std::atomic<int> last_primary_error{0};

  void RecordPrimary(int result) noexcept {
    primary_events.fetch_add(1, std::memory_order_relaxed);
    last_primary_result.store(result, std::memory_order_relaxed);
    if (result >= 0) {
      return;
    }

    primary_errors.fetch_add(1, std::memory_order_relaxed);
    int expected = 0;
    COROPACT_IGNORE_RESULT(
        first_primary_error.compare_exchange_strong(expected, result, std::memory_order_relaxed));
    last_primary_error.store(result, std::memory_order_relaxed);

    std::atomic<std::uint64_t>* counter = &primary_other_errors;
    switch (-result) {
      case ENOMEM:
        counter = &primary_enomem_errors;
        break;
      case EPIPE:
        counter = &primary_epipe_errors;
        break;
      case ECONNRESET:
        counter = &primary_connection_reset_errors;
        break;
      case ECANCELED:
        counter = &primary_cancelled_errors;
        break;
      default:
        break;
    }
    counter->fetch_add(1, std::memory_order_relaxed);
  }

  void RecordFailure(ZeroCopySendErrorKind kind) noexcept {
    errors.fetch_add(1, std::memory_order_relaxed);
    std::atomic<std::uint64_t>* counter = nullptr;
    switch (kind) {
      case ZeroCopySendErrorKind::kClosed:
        counter = &closed_errors;
        break;
      case ZeroCopySendErrorKind::kProfileUnavailable:
        counter = &profile_errors;
        break;
      case ZeroCopySendErrorKind::kBusy:
        counter = &busy_errors;
        break;
      case ZeroCopySendErrorKind::kSubmission:
        counter = &submission_errors;
        break;
      case ZeroCopySendErrorKind::kProtocol:
        counter = &protocol_errors;
        break;
    }
    counter->fetch_add(1, std::memory_order_relaxed);
  }

  void RecordNotification(int result, bool copied) noexcept {
    notification_events.fetch_add(1, std::memory_order_relaxed);
    last_notification_result.store(result, std::memory_order_relaxed);
    if (copied) {
      copied_completions.fetch_add(1, std::memory_order_relaxed);
    }
  }

  void RecordLogicalCompletion() noexcept {
    logical_completions.fetch_add(1, std::memory_order_relaxed);
  }

  void RecordCopyFallback() noexcept { copy_fallbacks.fetch_add(1, std::memory_order_relaxed); }
};

namespace detail {

struct ReadSomeForReadTag;
struct ReadSomeForTimeoutTag;

}  // namespace detail

class LUringStream {
public:
  COROPACT_DELETE_COPY(LUringStream);

  class ReadSomeAwaiter;
  class ReadSomeForAwaiter;
  class WriteSomeAwaiter;
  class WriteSomePartsAwaiter;
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

  [[nodiscard]]
  ReadSomeForAwaiter ReadSomeFor(std::span<std::byte> buffer,
                                 std::chrono::milliseconds timeout) noexcept;
  [[nodiscard]]
  WriteSomeAwaiter WriteSome(std::span<const std::byte> buffer) noexcept;

  [[nodiscard]]
  WriteSomePartsAwaiter WriteSome(std::span<const backend::WritePart> buffers) noexcept;

  // Optional extension consumed by io::WriteAll. The default remains the
  // ordinary WriteSome() path; enabled streams use SendZeroCopy() and keep
  // the caller's buffer alive until its notification CQE is observed.
  [[nodiscard]]
  bool ZeroCopyWritesEnabled() const noexcept {
    return zero_copy_writes_enabled_;
  }

  void SetZeroCopyWritesEnabled(bool enabled) noexcept { zero_copy_writes_enabled_ = enabled; }

  void SetZeroCopyDiagnostics(ZeroCopySendDiagnostics* diagnostics) noexcept {
    zero_copy_diagnostics_ = diagnostics;
  }

  // io::WriteAll uses this after SendZeroCopy() has fully released its buffer
  // ownership and retries ENOMEM with the regular send path.
  void RecordZeroCopyFallback() noexcept {
    if (zero_copy_diagnostics_ != nullptr) {
      zero_copy_diagnostics_->RecordCopyFallback();
    }
  }

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
  friend void detail::DispatchStreamCloseComplete(LUringOp* op) noexcept;

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
  ZeroCopySendDiagnostics* zero_copy_diagnostics_{nullptr};
  bool closed_{false};
};

class LUringStream::ReadSomeAwaiter : public detail::LUringOpHook<LUringStream::ReadSomeAwaiter> {
public:
  using OpHook = detail::LUringOpHook<ReadSomeAwaiter>;

  COROPACT_DELETE_COPY_MOVE(ReadSomeAwaiter);

  ReadSomeAwaiter(LUringStream& stream, std::span<std::byte> buffer) noexcept
      : OpHook(LUringOpKind::kReadComplete), stream_(&stream), buffer_(buffer) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  base::Result<std::size_t> await_resume() noexcept;

private:
  friend void detail::DispatchStreamReadComplete(LUringOp* op) noexcept;

  static void OnComplete(LUringOp* op) noexcept;

  LUringOp* Op() noexcept { return static_cast<OpHook*>(this); }

  LUringStream* stream_;
  std::span<std::byte> buffer_;
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
  friend void detail::DispatchTimedReadComplete(LUringOp* op) noexcept;
  friend void detail::DispatchTimedReadTimeoutComplete(LUringOp* op) noexcept;

  static void OnReadComplete(LUringOp* op) noexcept;
  static void OnTimeoutComplete(LUringOp* op) noexcept;

  LUringOp* ReadOp() noexcept { return static_cast<ReadOpHook*>(this); }
  LUringOp* TimeoutOp() noexcept { return static_cast<TimeoutOpHook*>(this); }

  void CompleteRead(LUringOp* current) noexcept;
  void CompleteTimeout(LUringOp* current) noexcept;
  void FinishIfReady(LUringOp* current) noexcept;

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
      : OpHook(LUringOpKind::kWriteComplete), stream_(&stream), buffer_(buffer) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  base::Result<std::size_t> await_resume() noexcept;

private:
  friend void detail::DispatchStreamWriteComplete(LUringOp* op) noexcept;

  static void OnComplete(LUringOp* op) noexcept;

  LUringOp* Op() noexcept { return static_cast<OpHook*>(this); }

  LUringStream* stream_;
  std::span<const std::byte> buffer_;
};

class LUringStream::WriteSomePartsAwaiter
    : public detail::LUringOpHook<LUringStream::WriteSomePartsAwaiter> {
public:
  using OpHook = detail::LUringOpHook<WriteSomePartsAwaiter>;

  COROPACT_DELETE_COPY_MOVE(WriteSomePartsAwaiter);

  WriteSomePartsAwaiter(LUringStream& stream, std::span<const backend::WritePart> buffers) noexcept
      : OpHook(LUringOpKind::kWritePartsComplete), stream_(&stream), buffers_(buffers) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  base::Result<std::size_t> await_resume() noexcept;

private:
  friend void detail::DispatchStreamWritePartsComplete(LUringOp* op) noexcept;

  static void OnComplete(LUringOp* op) noexcept;

  LUringOp* Op() noexcept { return static_cast<OpHook*>(this); }

  static constexpr std::size_t kMaxParts = 8;

  LUringStream* stream_;
  std::span<const backend::WritePart> buffers_;

  std::array<iovec, kMaxParts> iovecs_{};
  msghdr message_{};
};

class LUringStream::SendZeroCopyAwaiter
    : public detail::LUringOpHook<LUringStream::SendZeroCopyAwaiter> {
public:
  using OpHook = detail::LUringOpHook<SendZeroCopyAwaiter>;

  COROPACT_DELETE_COPY_MOVE(SendZeroCopyAwaiter);

  SendZeroCopyAwaiter(LUringStream& stream, std::span<const std::byte> buffer) noexcept
      : OpHook(LUringOpKind::kSendZeroCopyComplete),
        stream_(&stream),
        buffer_(buffer),
        diagnostics_(stream.zero_copy_diagnostics_) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  base::Result<ZeroCopySendResult> await_resume() noexcept;

private:
  friend CompletionDisposition detail::DispatchSendZeroCopyComplete(LUringOp* op,
                                                                    CompletionEvent event) noexcept;

  [[nodiscard]]
  static CompletionDisposition OnComplete(LUringOp* op, CompletionEvent event) noexcept;

  void RecordFailure(ZeroCopySendErrorKind kind) noexcept {
    if (diagnostics_ != nullptr) {
      diagnostics_->RecordFailure(kind);
    }
  }

  LUringOp* Op() noexcept { return static_cast<OpHook*>(this); }

  LUringStream* stream_;
  std::span<const std::byte> buffer_;
  ZeroCopySendDiagnostics* diagnostics_;
  operation::detail::SplitReleaseLifecycle lifecycle_;
  bool copied_{false};
  bool notification_received_{false};
};

static_assert(backend::AsyncStream<LUringStream>);

}  // namespace coropact::luring
