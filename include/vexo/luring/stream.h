#pragma once

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
  VEXO_DELETE_COPY_MOVE(LUringStream);

  LUringStream(LUringLoop* loop, int fd, net::InetAddress peer) noexcept;
  ~LUringStream();

  coro::Task<base::Result<std::size_t>> ReadSome(std::span<std::byte> buffer);
  coro::Task<base::Result<std::size_t>> WriteSome(std::span<const std::byte> buffer);
  coro::Task<base::Result<void>> Shutdown();
  coro::Task<base::Result<void>> Close();

  const net::InetAddress& PeerAddress() const noexcept { return peer_; }
  int fd() const noexcept { return fd_; }

private:
  class ReadAwaiter;
  class WriteAwaiter;
  class CloseAwaiter;

  void NotifyCloseProgress() noexcept;

  LUringLoop* loop_;
  int fd_{-1};
  net::InetAddress peer_;
  ReadAwaiter* pending_read_{nullptr};
  WriteAwaiter* pending_write_{nullptr};
  CloseAwaiter* pending_close_{nullptr};
  bool closed_{false};
};

static_assert(io::AsyncStream<LUringStream>);

}  // namespace vexo::luring
