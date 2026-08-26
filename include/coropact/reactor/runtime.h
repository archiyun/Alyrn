// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <functional>

#include "coropact/coro/detached_task.h"
#include "coropact/net/endpoint.h"
#include "coropact/reactor/stream.h"
#include "coropact/runtime.h"

namespace coropact {

// Compile-time Reactor binding for the backend-neutral Runtime composition
// root. Its handler keeps the accepted stream statically typed.
template <>
class Runtime::Builder<runtime::Reactor> {
public:
  // Runtime transfers each accepted stream to the handler by value. The
  // detached handler coroutine owns that stream until it finishes.
  using ConnectionHandler = std::function<coro::DetachedTask(reactor::Stream)>;

  explicit Builder(net::Endpoint listen_addr) noexcept;

  // Selects independent Loop workers. One is the conservative default;
  // AutoWorkers() is opt-in.
  Builder& Workers(std::size_t count) noexcept;
  Builder& AutoWorkers() noexcept;
  Builder& OnConnection(ConnectionHandler handler);

  [[nodiscard]] Runtime Build();

private:
  net::Endpoint listen_addr_;
  std::size_t worker_count_{1};
  ConnectionHandler connection_handler_;
};

}  // namespace coropact
