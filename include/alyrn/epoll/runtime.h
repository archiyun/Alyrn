// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <functional>

#include "alyrn/spawn.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/tcp_options.h"
#include "alyrn/epoll/loop.h"
#include "alyrn/epoll/stream.h"
#include "alyrn/runtime.h"

namespace alyrn {

// Compile-time Epoll binding for the backend-neutral Runtime composition
// root. Its handler keeps the accepted stream statically typed.
template <>
class Runtime::Builder<runtime::Epoll> {
public:
  // Runtime transfers each accepted stream to the handler by value. The
  // detached handler coroutine owns that stream until it finishes.
  using ConnectionHandler = std::function<DetachedTask(epoll::Stream)>;

  // Runs once on each worker thread, inside that worker's Loop scheduling
  // context, after the Loop and listener exist and before the first
  // connection is accepted. Create per-worker state such as Channels here.
  // An error fails Start()/Run() with that error.
  using WorkerStartHook = std::function<Result<void>(epoll::Loop&, std::size_t worker_index)>;
  // Runs once on each worker thread whose start hook succeeded, inside that
  // worker's Loop scheduling context, after the listener stopped, pending I/O
  // was canceled, and connection coroutines drained, and before the Loop is
  // destroyed. Close per-worker Channels here. Work the hook schedules, such
  // as waiters woken by Channel::Close(), drains after it returns, so objects
  // those waiters reference must outlive the hook. It must not start new I/O.
  using WorkerStopHook = std::function<void(epoll::Loop&, std::size_t worker_index)>;

  explicit Builder(net::Endpoint listen_addr) noexcept;

  // Selects independent Loop workers. One is the conservative default;
  // AutoWorkers() is opt-in.
  Builder& Workers(std::size_t count) noexcept;
  Builder& AutoWorkers() noexcept;
  // Socket options applied to every accepted stream. Request/response
  // protocols usually want no_delay: otherwise a small reply written right
  // after another one can wait for the peer's delayed ACK (Nagle).
  Builder& Tcp(net::TcpOptions options) noexcept;
  Builder& OnConnection(ConnectionHandler handler);
  Builder& OnWorkerStart(WorkerStartHook hook);
  Builder& OnWorkerStop(WorkerStopHook hook);

  [[nodiscard]] Runtime Build();

private:
  net::Endpoint listen_addr_;
  std::size_t worker_count_{1};
  net::TcpOptions tcp_options_{};
  ConnectionHandler connection_handler_;
  WorkerStartHook worker_start_hook_;
  WorkerStopHook worker_stop_hook_;
};

}  // namespace alyrn
