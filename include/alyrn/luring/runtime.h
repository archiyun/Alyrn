// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <functional>

#include "alyrn/coro/detached_task.h"
#include "alyrn/luring/stream.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/runtime.h"

namespace alyrn {

// Compile-time io_uring binding for the backend-neutral Runtime composition
// root. Ring and operation-specific tuning stay backend implementation policy.
template <>
class Runtime::Builder<runtime::LUring> {
public:
  // Runtime transfers each accepted stream to the handler by value. The
  // detached handler coroutine owns that stream until it finishes.
  using ConnectionHandler = std::function<coro::DetachedTask(luring::Stream)>;

  explicit Builder(net::Endpoint listen_addr) noexcept;

  Builder& Workers(std::size_t count) noexcept;
  Builder& AutoWorkers() noexcept;
  Builder& OnConnection(ConnectionHandler handler);

  [[nodiscard]] Runtime Build();

private:
  net::Endpoint listen_addr_;
  std::size_t worker_count_{1};
  ConnectionHandler connection_handler_;
};

}  // namespace alyrn
