// Copyright (c) 2026 Arsenova. All Rights Reserved.
#pragma once

#include <functional>
#include <memory>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/luring/stream.h"
#include "vexo/luring/worker_group.h"
#include "vexo/net/inet_address.h"
#include "vexo/utils/macros.h"

namespace vexo::luring {

struct LUringServerOptions {
  LUringWorkerGroupOptions worker_group_options{};
};

class LUringServer {
public:
  VEXO_DELETE_COPY_MOVE(LUringServer);

  using Stream = LUringStream;
  using ThreadInitCallback = LUringWorkerGroup::ThreadInitCallback;
  using SessionHandler = std::function<coro::Task<void>(LUringLoop&, std::unique_ptr<Stream>)>;

  explicit LUringServer(net::InetAddress listen_addr, LUringServerOptions options = {});

  void set_thread_init_callback(ThreadInitCallback callback) noexcept {
    thread_init_callback_ = std::move(callback);
  }
  void set_session_handler(SessionHandler handler) noexcept {
    session_handler_ = std::move(handler);
  }

  [[nodiscard]] base::Result<void> Start();
  void Stop() noexcept;

  [[nodiscard]] bool started() const noexcept { return started_; }

private:
  net::InetAddress listen_addr_;
  LUringServerOptions options_{};

  ThreadInitCallback thread_init_callback_;
  SessionHandler session_handler_;

  std::unique_ptr<LUringWorkerGroup> workers_;
  bool started_{false};
};

}  // namespace vexo::luring
