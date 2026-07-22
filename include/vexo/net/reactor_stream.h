// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <sys/uio.h>

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <optional>
#include <span>
#include <vector>

#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/task.h"
#include "vexo/io/buffer.h"
#include "vexo/net/channel.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/socket.h"
#include "vexo/utils/macros.h"

namespace vexo::net {

class ReactorStream {
public:
  VEXO_DELETE_COPY(ReactorStream);

  ReactorStream(EventLoop* loop, int fd, InetAddress peer = InetAddress(0));
  ~ReactorStream();

  // Moves are loop-affine: the source must be used from its owning loop
  // thread and must not have a pending read or write operation.
  ReactorStream(ReactorStream&& other) noexcept;
  ReactorStream& operator=(ReactorStream&& other) noexcept;

  class ReadSomeAwaiter;
  class WriteSomeAwaiter;
  class BufferReadAwaiter;
  class BufferWriteAwaiter;

  [[nodiscard]] ReadSomeAwaiter ReadSome(std::span<std::byte> buffer) noexcept;
  [[nodiscard]] BufferReadAwaiter ReadSome(io::Buffer& buffer, std::size_t reserve = 4096) noexcept;
  [[nodiscard]] ReadSomeAwaiter ReadSomeFor(std::span<std::byte> buffer,
                                            std::chrono::milliseconds timeout) noexcept;
  [[nodiscard]] BufferReadAwaiter ReadSomeFor(io::Buffer& buffer, std::chrono::milliseconds timeout,
                                              std::size_t reserve = 4096) noexcept;
  [[nodiscard]] WriteSomeAwaiter WriteSome(std::span<const std::byte> buffer) noexcept;
  [[nodiscard]] BufferWriteAwaiter WriteSome(io::Buffer& buffer) noexcept;
  coro::Task<base::Result<void>> Shutdown();
  coro::Task<base::Result<void>> Close();

  [[nodiscard]] const InetAddress& PeerAddress() const noexcept { return peer_; }

private:
  class ReadOperation {
  public:
    virtual ~ReadOperation() = default;
    virtual void Complete(base::Result<std::size_t> result) noexcept = 0;
    virtual void OnReady() noexcept = 0;
  };

  class WriteOperation {
  public:
    virtual ~WriteOperation() = default;
    virtual void Complete(base::Result<std::size_t> result) noexcept = 0;
    virtual void OnReady() noexcept = 0;
  };

  void HandleRead(vexo::time::Timestamp receive_time);
  void HandleWrite();
  void HandleClose();
  void HandleError();

  void CompleteRead(base::Result<std::size_t> result);
  void CompleteWrite(base::Result<std::size_t> result);
  void DetachChannel();
  void BindChannelCallbacks() noexcept;
  void ResetForMove() noexcept;
  static EventLoop* PrepareMove(ReactorStream& other) noexcept;

  EventLoop* loop_;
  Socket socket_;
  Channel channel_;
  InetAddress peer_;
  ReadOperation* pending_read_{nullptr};
  WriteOperation* pending_write_{nullptr};
  bool closed_{false};
};

class ReactorStream::ReadSomeAwaiter : public ReactorStream::ReadOperation {
public:
  VEXO_DELETE_COPY_MOVE(ReadSomeAwaiter);

  ReadSomeAwaiter(ReactorStream& stream, std::span<std::byte> buffer,
                  std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) noexcept
      : stream_(&stream), buffer_(buffer), timeout_(timeout) {}

  [[nodiscard]] bool await_ready() const noexcept { return false; }
  [[nodiscard]] bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  void Complete(base::Result<std::size_t> result) noexcept override;
  void OnReady() noexcept override;

  ReactorStream* stream_;
  std::span<std::byte> buffer_;
  std::chrono::milliseconds timeout_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_;
  std::optional<base::Result<std::size_t>> result_;
  time::TimerId timer_;
  bool timer_armed_{false};
};

class ReactorStream::WriteSomeAwaiter : public ReactorStream::WriteOperation {
public:
  VEXO_DELETE_COPY_MOVE(WriteSomeAwaiter);

  WriteSomeAwaiter(ReactorStream& stream, std::span<const std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  [[nodiscard]] bool await_ready() const noexcept { return false; }
  [[nodiscard]] bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  void Complete(base::Result<std::size_t> result) noexcept override;
  void OnReady() noexcept override;

  ReactorStream* stream_;
  std::span<const std::byte> buffer_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_;
  std::optional<base::Result<std::size_t>> result_;
};

class ReactorStream::BufferReadAwaiter : public ReactorStream::ReadOperation {
public:
  VEXO_DELETE_COPY_MOVE(BufferReadAwaiter);

  BufferReadAwaiter(ReactorStream& stream, io::Buffer& buffer, std::size_t reserve,
                    std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) noexcept;

  [[nodiscard]] bool await_ready() const noexcept { return false; }
  [[nodiscard]] bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  void Complete(base::Result<std::size_t> result) noexcept override;
  void OnReady() noexcept override;
  bool PrepareReservation() noexcept;
  void FinishAttempt(base::Result<std::size_t> result) noexcept;

  ReactorStream* stream_;
  io::Buffer* buffer_;
  std::size_t reserve_;
  std::chrono::milliseconds timeout_;
  std::vector<iovec> iovs_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_;
  std::optional<base::Result<std::size_t>> result_;
  vexo::time::TimerId timer_;
  bool timer_armed_{false};
};

class ReactorStream::BufferWriteAwaiter : public ReactorStream::WriteOperation {
public:
  VEXO_DELETE_COPY_MOVE(BufferWriteAwaiter);

  BufferWriteAwaiter(ReactorStream& stream, io::Buffer& buffer) noexcept;

  [[nodiscard]] bool await_ready() const noexcept { return false; }
  [[nodiscard]] bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  base::Result<std::size_t> await_resume() noexcept;

private:
  void Complete(base::Result<std::size_t> result) noexcept override;
  void OnReady() noexcept override;
  bool PrepareReadable() noexcept;
  void FinishAttempt(base::Result<std::size_t> result) noexcept;

  ReactorStream* stream_;
  io::Buffer* buffer_;
  std::vector<iovec> iovs_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_;
  std::optional<base::Result<std::size_t>> result_;
};

}  // namespace vexo::net
