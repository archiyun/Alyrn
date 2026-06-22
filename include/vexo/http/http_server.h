// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "vexo/base/noncopyable.h"
#include "vexo/http/router.h"
#include "vexo/net/tcp_server.h"
#include "vexo/task/blocking_executor.h"

#include <memory>
#include <source_location>
#include <string>
#include <string_view>

namespace vexo::http {

// HttpServer adds a minimal HTTP/1.1 processing layer on top of TcpServer.
class HttpServer : public vexo::base::NonCopyable {
 public:
  using TcpConnectionPtr = vexo::net::TcpConnection::TcpConnectionPtr;

  HttpServer(vexo::net::EventLoop* loop, const vexo::net::InetAddress& addr,
             std::string name);

  void set_thread_num(int num_threads);

  // Delegates to the underlying TcpServer. Must be called before Start().
  void set_edge_triggered(bool et);

  void set_blocking_executor(
      std::shared_ptr<vexo::task::BlockingExecutor> executor);

  // Registers GET /metrics → JSON snapshot of executor counters.
  // Must be called after set_blocking_executor().
  void RegisterMetricsRoute();

  // Registers routes by forwarding to Router. The default-argument
  // std::source_location captures the user's call site so registration errors
  // (logged & aborted inside Router) point at user code rather than this
  // forwarding shim.
  void Get(std::string_view path, Handler handler,
           std::source_location loc = std::source_location::current());
  void Post(std::string_view path, Handler handler,
            std::source_location loc = std::source_location::current());
  void Add(Method method, std::string_view path, Handler handler,
           std::source_location loc = std::source_location::current());

  void Start();

 private:
  // Initializes per-connection HTTP parsing state.
  void OnConnection(const TcpConnectionPtr& conn);

  // Parses bytes, dispatches handlers, and writes responses.
  void OnMessage(const TcpConnectionPtr& conn, vexo::net::Buffer& buf,
                 vexo::time::Timestamp ts);

  // Builds a JSON error response for common HTTP failures.
  HttpResponse MakeError(StatusCode code, std::string_view message) const;

  vexo::net::TcpServer server_;
  Router router_;
  std::shared_ptr<vexo::task::BlockingExecutor> blocking_executor_;
};

}  // namespace vexo::http
