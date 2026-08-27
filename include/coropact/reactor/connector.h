// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <string_view>

#include "coropact/result.h"
#include "coropact/coro/task.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/tcp_options.h"
#include "coropact/reactor/loop.h"
#include "coropact/reactor/stream.h"
#include "coropact/time/clock.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

struct ConnectorOptions {
  // Applies to every Stream returned by Connect.
  StreamOptions stream_options{};
  net::TcpOptions tcp_options{};
};

class Connector {
public:
  COROPACT_DELETE_COPY(Connector);

  using StreamType = Stream;

  [[nodiscard]]
  static Result<Connector> Create(Loop* loop,
                                               ConnectorOptions options = {}) noexcept;

  explicit Connector(Loop* loop, ConnectorOptions options = {}) noexcept;

  Connector(Connector&& other) noexcept;
  Connector& operator=(Connector&& other) noexcept;

  // Connect is loop-affine. Independent calls may be pending concurrently;
  // each call owns its socket, Channel, result, and continuation.
  coro::Task<Result<Stream>> Connect(net::Endpoint peer);
  coro::Task<Result<Stream>> Connect(std::string_view host, std::uint16_t port);
  coro::Task<void> SleepFor(time::Duration delay);

private:
  void RequireOwnerLoop() const noexcept;

  Loop* loop_;
  ConnectorOptions options_;
};

}  // namespace coropact::reactor
