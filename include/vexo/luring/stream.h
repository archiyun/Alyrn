#pragma once

#include <chrono>
#include <cstddef>
#include <span>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/io/async_stream.h"
#include "vexo/net/inet_address.h"
#include "vexo/utils/macros.h"

namespace vexo::luring {

class LUringLoop;

class LUringStream {
public:
  VEXO_DELETE_COPY(LUringStream);

  LUringStream(LUringLoop* loop, int fd, net::InetAddress peer) noexcept;
  ~LUringStream();

  // A stream may move only on its owning loop thread and only while no
  // operation is waiting for a CQE.
  LUringStream(LUringStream&& other) noexcept;
  LUringStream& operator=(LUringStream&& other) noexcept;

  coro::Task<base::Result<std::size_t>> ReadSome(std::span<std::byte> buffer);
  coro::Task<base::Result<std::size_t>> ReadSomeFor(std::span<std::byte> buffer,
                                                    std::chrono::milliseconds timeout);
  coro::Task<base::Result<std::size_t>> WriteSome(std::span<const std::byte> buffer);
  coro::Task<base::Result<void>> Shutdown();
  coro::Task<base::Result<void>> Close();

  const net::InetAddress& PeerAddress() const noexcept { return peer_; }
  int fd() const noexcept { return fd_; }

private:
  class ReadAwaiter;
  class ReadForAwaiter;
  class WriteAwaiter;
  class CloseAwaiter;

  void NotifyCloseProgress() noexcept;
  void ResetForMove() noexcept;
  static LUringLoop* PrepareMove(LUringStream& other) noexcept;

  LUringLoop* loop_;
  int fd_{-1};
  net::InetAddress peer_;
  void* pending_read_{nullptr};
  WriteAwaiter* pending_write_{nullptr};
  CloseAwaiter* pending_close_{nullptr};
  bool closed_{false};
};

static_assert(io::AsyncStream<LUringStream>);

}  // namespace vexo::luring
