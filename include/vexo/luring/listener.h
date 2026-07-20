#pragma once

#include <sys/socket.h>

#include <cstddef>
#include <memory>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/io/async_listener.h"
#include "vexo/luring/stream.h"
#include "vexo/net/inet_address.h"
#include "vexo/utils/macros.h"

namespace vexo::luring {

class LUringLoop;

struct LUringListenOptions {
  bool reuse_addr{true};
  bool reuse_port{true};
  int backlog{SOMAXCONN};
  // Number of accepts kept in flight by each worker. A value greater than one
  // prevents a connection burst from being serialized behind one accept CQE.
  std::size_t accept_depth{4};
};

class LUringListener {
public:
  VEXO_DELETE_COPY_MOVE(LUringListener);
  using Stream = LUringStream;

  static base::Result<std::unique_ptr<LUringListener>> Create(
      LUringLoop* loop, const net::InetAddress& listen_addr,
      LUringListenOptions options = {}) noexcept;

  ~LUringListener();

  coro::Task<base::Result<std::unique_ptr<LUringStream>>> Accept();
  coro::Task<base::Result<void>> Close();

  base::Result<net::InetAddress> LocalAddress() const noexcept;
  int fd() const noexcept { return fd_; }

private:
  class AcceptAwaiter;
  class CloseAwaiter;

  LUringListener(LUringLoop* loop, int fd) noexcept;
  void NotifyCloseProgress() noexcept;

  LUringLoop* loop_;
  int fd_{-1};
  std::size_t pending_accepts_{0};
  CloseAwaiter* pending_close_{nullptr};
  bool closed_{false};
};

static_assert(io::AsyncListener<LUringListener>);

}  // namespace vexo::luring
