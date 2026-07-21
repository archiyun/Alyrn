// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <optional>
#include <span>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/io/async_stream.h"
#include "vexo/luring/op.h"
#include "vexo/net/inet_address.h"
#include "vexo/utils/macros.h"

namespace vexo::luring {

class LUringLoop;

class LUringStream {
public:
  VEXO_DELETE_COPY(LUringStream);

  class ReadSomeAwaiter;
  class WriteSomeAwaiter;

  LUringStream(LUringLoop* loop, int fd, net::InetAddress peer) noexcept;
  ~LUringStream();

  // A stream may move only on its owning loop thread and only while no
  // operation is waiting for a CQE.
  LUringStream(LUringStream&& other) noexcept;
  LUringStream& operator=(LUringStream&& other) noexcept;

  [[nodiscard]] ReadSomeAwaiter ReadSome(std::span<std::byte> buffer) noexcept;
  coro::Task<base::Result<std::size_t>> ReadSomeFor(std::span<std::byte> buffer,
                                                    std::chrono::milliseconds timeout);
  [[nodiscard]] WriteSomeAwaiter WriteSome(std::span<const std::byte> buffer) noexcept;
  coro::Task<base::Result<void>> Shutdown();
  coro::Task<base::Result<void>> Close();

  [[nodiscard]] const net::InetAddress& PeerAddress() const noexcept { return peer_; }
  [[nodiscard]] int fd() const noexcept { return fd_; }

private:
  class ReadForAwaiter;
  class CloseAwaiter;

  void NotifyCloseProgress() noexcept;
  void ResetForMove() noexcept;
  static LUringLoop* PrepareMove(LUringStream& other) noexcept;

  LUringLoop* loop_;
  int fd_{-1};
  net::InetAddress peer_;
  void* pending_read_{nullptr};
  WriteSomeAwaiter* pending_write_{nullptr};
  CloseAwaiter* pending_close_{nullptr};
  bool closed_{false};
};

class LUringStream::ReadSomeAwaiter {
public:
  VEXO_DELETE_COPY_MOVE(ReadSomeAwaiter);

  ReadSomeAwaiter(LUringStream& stream, std::span<std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  [[nodiscard]] bool await_ready() const noexcept { return false; }
  [[nodiscard]] bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  static void OnComplete(LUringOp* op) noexcept;

  LUringStream* stream_;
  std::span<std::byte> buffer_;
  LUringOp op_{.kind = LUringOpKind::kRead};
  std::optional<base::Result<std::size_t>> immediate_;
};

class LUringStream::WriteSomeAwaiter {
public:
  VEXO_DELETE_COPY_MOVE(WriteSomeAwaiter);

  WriteSomeAwaiter(LUringStream& stream, std::span<const std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  [[nodiscard]] bool await_ready() const noexcept { return false; }
  [[nodiscard]] bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  static void OnComplete(LUringOp* op) noexcept;

  LUringStream* stream_;
  std::span<const std::byte> buffer_;
  LUringOp op_{.kind = LUringOpKind::kWrite};
  std::optional<base::Result<std::size_t>> immediate_;
};

static_assert(io::AsyncStream<LUringStream>);

}  // namespace vexo::luring
