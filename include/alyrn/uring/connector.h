// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <string_view>

#include "alyrn/backend/async_connector.h"
#include "alyrn/detail/macros.h"
#include "alyrn/net/tcp_options.h"
#include "alyrn/result.h"
#include "alyrn/task.h"
#include "alyrn/time/clock.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/stream.h"

namespace alyrn::uring {

struct ConnectorOptions {
  net::TcpOptions tcp_options;
};

class Connector {
public:
  ALYRN_DELETE_COPY(Connector);

  using StreamType = Stream;

  Connector(Connector&&) noexcept;
  Connector& operator=(Connector&&) noexcept;

  [[nodiscard]]
  static Result<Connector> Create(Loop* loop, ConnectorOptions options = {}) noexcept;
  explicit Connector(Loop* loop, ConnectorOptions options = {}) noexcept;

  // Connect is loop-affine. Independent calls may be pending concurrently;
  // each call owns its socket, physical request, result, and continuation.
  Task<Result<Stream>> Connect(std::string_view host, std::uint16_t port);

  // Backend-selected timer for application-level health-check loops.
  Task<void> SleepFor(time::Duration delay);

private:
  void RequireOwnerLoop() const noexcept;

  Loop* loop_;
  ConnectorOptions options_;
};

static_assert(backend::AsyncConnector<Connector>);

}  // namespace alyrn::uring
