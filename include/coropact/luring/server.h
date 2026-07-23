// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <functional>
#include <memory>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/luring/stream.h"
#include "coropact/luring/worker_group.h"
#include "coropact/net/inet_address.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

struct LUringServerOptions {
  LUringWorkerGroupOptions worker_group_options{};
};

class LUringServer {
public:
  COROPACT_DELETE_COPY_MOVE(LUringServer);

  using Stream = LUringStream;
  using ThreadInitCallback = LUringWorkerGroup::ThreadInitCallback;
  using SessionHandler = std::function<coro::Task<void>(LUringWorkerContext&, Stream)>;

  explicit LUringServer(net::InetAddress listen_addr, LUringServerOptions options = {});
  ~LUringServer() noexcept;

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

}  // namespace coropact::luring
