// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <functional>
#include <memory>

#include "coropact/result.h"
#include "coropact/coro/detached_task.h"
#include "coropact/luring/stream.h"
#include "coropact/luring/detail/worker_group.h"
#include "coropact/net/endpoint.h"
#include "coropact/utils/macros.h"

namespace coropact::luring::detail {

struct LUringServerOptions {
  LUringWorkerGroupOptions worker_group_options{};
};

// High-level TCP server facade backed by a group of coroutine-driven
// io_uring workers. Start() creates one LUringLoop, io_uring ring, listener,
// and connector per worker; SO_REUSEPORT lets the kernel distribute incoming
// connections across those independent listeners. Each accepted LUringStream
// is passed to SessionHandler on the owning worker's loop thread, and the
// returned coroutine is detached for the lifetime of that session.
//
// The server owns the worker group after a successful Start(). Stop() requests
// all workers to stop, drains their loop-bound operations, and joins their
// threads before returning.
class LUringServer {
public:
  COROPACT_DELETE_COPY_MOVE(LUringServer);

  using Stream = LUringStream;
  using ThreadInitCallback = LUringWorkerGroup::ThreadInitCallback;
  using ThreadExitCallback = LUringWorkerGroup::ThreadExitCallback;
  using SessionHandler =
      std::function<coro::DetachedTask(LUringWorkerContext&, Stream)>;

  explicit LUringServer(net::Endpoint listen_addr, LUringServerOptions options = {});
  ~LUringServer() noexcept;

  void SetThreadInitCallback(ThreadInitCallback callback) noexcept {
    thread_init_callback_ = std::move(callback);
  }
  void SetThreadExitCallback(ThreadExitCallback callback) noexcept {
    thread_exit_callback_ = std::move(callback);
  }
  void SetSessionHandler(SessionHandler handler) noexcept {
    session_handler_ = std::move(handler);
  }

  [[nodiscard]]
  Result<void> Start();

  void Stop() noexcept;

  [[nodiscard]]
  bool Started() const noexcept {
    return started_;
  }

private:
  net::Endpoint listen_addr_;
  LUringServerOptions options_{};

  ThreadInitCallback thread_init_callback_;
  ThreadExitCallback thread_exit_callback_;
  SessionHandler session_handler_;

  std::unique_ptr<LUringWorkerGroup> workers_;
  bool started_{false};
};

}  // namespace coropact::luring::detail
