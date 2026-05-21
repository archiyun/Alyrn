// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/http/router.h"
#include "runtime/net/tcp_server.h"
#include "runtime/task/scheduler.h"
#ifdef RUNTIME_ENABLE_SSL
#include "runtime/net/ssl_context.h"
#endif

#include <memory>
#include <source_location>
#include <string>
#include <string_view>

namespace runtime::http {

// HttpServer adds a minimal HTTP/1.1 processing layer on top of TcpServer.
class HttpServer : public runtime::base::NonCopyable {
public:
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;

  HttpServer(runtime::net::EventLoop* loop,
             const runtime::net::InetAddress& addr,
             std::string name);

#ifdef RUNTIME_ENABLE_SSL
  void SetTls(runtime::net::SslContext* ctx);
#endif

  void SetThreadNum(int num_threads);

  // Delegates to the underlying TcpServer. Must be called before Start().
  void SetEdgeTriggered(bool et);

  void SetScheduler(std::shared_ptr<runtime::task::Scheduler> sched);

  // Registers GET /metrics → JSON snapshot of scheduler counters.
  // Must be called after SetScheduler().
  void RegisterMetricsRoute();

  // Registers GET /debug/tasks → JSON array of recent completed tasks.
  // Must be called after SetScheduler().
  void RegisterDebugTasksRoute();

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
  void OnMessage(const TcpConnectionPtr& conn,
                 runtime::net::Buffer& buf,
                 runtime::time::Timestamp ts);

  // Builds a JSON error response for common HTTP failures.
  HttpResponse MakeError(StatusCode code, std::string_view message) const;

  runtime::net::TcpServer server_;
  Router router_;
  std::shared_ptr<runtime::task::Scheduler> scheduler_;
#ifdef RUNTIME_ENABLE_SSL
  runtime::net::SslContext* ssl_ctx_{nullptr};
#endif
};

}  // namespace runtime::http
