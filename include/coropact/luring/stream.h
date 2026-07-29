// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <linux/time_types.h>
#include <sys/socket.h>

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <span>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/io/async_stream.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/op_hook.h"
#include "coropact/luring/op.h"
#include "coropact/net/endpoint.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

class LUringLoop;

struct ZeroCopySendResult {
  std::size_t bytes{0};
  bool copied{false};
  // False means this send completed with its primary CQE and did not require
  // a separate F_NOTIF notification. When true, the notification CQE has
  // already been observed before SendZeroCopy() returned.
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

  [[nodiscard]]
  ReadSomeAwaiter ReadSome(std::span<std::byte> buffer) noexcept;

  [[nodiscard]]
  ReadSomeForAwaiter ReadSomeFor(std::span<std::byte> buffer,
                                 std::chrono::milliseconds timeout) noexcept;
  [[nodiscard]]
  WriteSomeAwaiter WriteSome(std::span<const std::byte> buffer) noexcept;

  [[nodiscard]]
  WriteSomePartsAwaiter WriteSome(std::span<const io::WritePart> buffers) noexcept;

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

private:
  friend void detail::DispatchStreamCloseComplete(LUringOp* op) noexcept;

  class CloseAwaiter;

  void NotifyCloseProgress() noexcept;
  void ResetForMove() noexcept;
  static LUringLoop* PrepareMove(LUringStream& other) noexcept;

  LUringLoop* loop_;
  int fd_{-1};
  net::Endpoint peer_;
  void* pending_read_{nullptr};
  void* pending_write_{nullptr};
  CloseAwaiter* pending_close_{nullptr};
  bool closed_{false};
};

class LUringStream::ReadSomeAwaiter
    : public detail::LUringOpHook<LUringStream::ReadSomeAwaiter> {
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
    : public detail::LUringOpHook<LUringStream::ReadSomeForAwaiter,
                                  detail::ReadSomeForReadTag>,
      public detail::LUringOpHook<LUringStream::ReadSomeForAwaiter,
                                  detail::ReadSomeForTimeoutTag> {
public:
  using ReadOpHook =
      detail::LUringOpHook<ReadSomeForAwaiter, detail::ReadSomeForReadTag>;
  using TimeoutOpHook =
      detail::LUringOpHook<ReadSomeForAwaiter, detail::ReadSomeForTimeoutTag>;

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
  bool read_done_{false};
  bool timeout_done_{false};
};

class LUringStream::WriteSomeAwaiter
    : public detail::LUringOpHook<LUringStream::WriteSomeAwaiter> {
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

  WriteSomePartsAwaiter(LUringStream& stream, std::span<const io::WritePart> buffers) noexcept
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
  std::span<const io::WritePart> buffers_;

  std::array<iovec, kMaxParts> iovecs_{};
  msghdr message_{};

};

class LUringStream::SendZeroCopyAwaiter
    : public detail::LUringOpHook<LUringStream::SendZeroCopyAwaiter> {
public:
  using OpHook = detail::LUringOpHook<SendZeroCopyAwaiter>;

  COROPACT_DELETE_COPY_MOVE(SendZeroCopyAwaiter);

  SendZeroCopyAwaiter(
      LUringStream& stream,
      std::span<const std::byte> buffer) noexcept
      : OpHook(LUringOpKind::kSendZeroCopyComplete),
        stream_(&stream),
        buffer_(buffer) {}

  [[nodiscard]]
  bool await_ready() const noexcept { return false; }

  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;

  base::Result<ZeroCopySendResult> await_resume() noexcept;

private:
  friend void detail::DispatchSendZeroCopyComplete(
      LUringOp* op,
      CompletionEvent event) noexcept;

  static void OnComplete(LUringOp* op, CompletionEvent event) noexcept;

  LUringOp* Op() noexcept { return static_cast<OpHook*>(this); }

  LUringStream* stream_;
  std::span<const std::byte> buffer_;
  bool primary_seen_{false};
  bool copied_{false};
  bool notification_received_{false};
};

static_assert(io::AsyncStream<LUringStream>);

}  // namespace coropact::luring
