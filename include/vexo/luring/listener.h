#pragma once

#include <memory>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/io/async_listener.h"
#include "vexo/luring/stream.h"
#include "vexo/net/inet_address.h"
#include "vexo/utils/macros.h"

namespace vexo::luring {

class LUringLoop;

class LUringListener {
public:
  VEXO_DELETE_COPY_MOVE(LUringListener);
  using Stream = LUringStream;

  static base::Result<std::unique_ptr<LUringListener>> Create(
      LUringLoop* loop, const net::InetAddress& listen_addr) noexcept;

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
  AcceptAwaiter* pending_accept_{nullptr};
  CloseAwaiter* pending_close_{nullptr};
  bool closed_{false};
};

static_assert(io::AsyncListener<LUringListener>);

}  // namespace vexo::luring
