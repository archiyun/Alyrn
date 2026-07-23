// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <sys/socket.h>

#include <cstddef>

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
  VEXO_DELETE_COPY(LUringListener);

  using Stream = LUringStream;

  static base::Result<LUringListener> Create(LUringLoop* loop, const net::InetAddress& listen_addr,
                                             LUringListenOptions options = {}) noexcept;

  ~LUringListener();

  // A listener may move only on its owning loop thread and only while no
  // accept or close operation is waiting for a CQE.
  LUringListener(LUringListener&& other) noexcept;
  LUringListener& operator=(LUringListener&& other) noexcept;

  coro::Task<base::Result<LUringStream>> Accept();
  coro::Task<base::Result<void>> Close();

  [[nodiscard]] base::Result<net::InetAddress> LocalAddress() const noexcept;
  [[nodiscard]] int fd() const noexcept { return fd_; }

private:
  class AcceptAwaiter;
  class CloseAwaiter;

  [[nodiscard]] LUringListener(LUringLoop* loop, int fd) noexcept;
  void NotifyCloseProgress() noexcept;
  void ResetForMove() noexcept;
  static LUringLoop* PrepareMove(LUringListener& other) noexcept;

  LUringLoop* loop_;
  int fd_{-1};
  std::size_t pending_accepts_{0};
  CloseAwaiter* pending_close_{nullptr};
  bool closed_{false};
};

static_assert(io::AsyncListener<LUringListener>);

}  // namespace vexo::luring
