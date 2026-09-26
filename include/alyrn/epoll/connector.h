// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <string_view>

#include "alyrn/backend/async_connector.h"
#include "alyrn/detail/macros.h"
#include "alyrn/epoll/loop.h"
#include "alyrn/epoll/stream.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/tcp_options.h"
#include "alyrn/result.h"
#include "alyrn/task.h"
#include "alyrn/time/clock.h"

namespace alyrn::epoll {

struct ConnectorOptions {
  // Applies to every Stream returned by Connect.
  StreamOptions stream_options{};
  net::TcpOptions tcp_options{};
};

class Connector {
public:
  ALYRN_DELETE_COPY(Connector);

  using StreamType = Stream;

  [[nodiscard]]
  static Result<Connector> Create(Loop* loop, ConnectorOptions options = {}) noexcept;

  explicit Connector(Loop* loop, ConnectorOptions options = {}) noexcept;

  Connector(Connector&& other) noexcept;
  Connector& operator=(Connector&& other) noexcept;

  // Connect is loop-affine. Independent calls may be pending concurrently;
  // each call owns its socket, Channel, result, and continuation.
  Task<Result<Stream>> Connect(net::Endpoint peer);
  // Connects to a numeric IPv4 or IPv6 literal such as "127.0.0.1" or "::1".
  // Host names, including "localhost", are not resolved: they complete with
  // EINVAL. Resolve names before connecting; a blocking resolver would stall
  // the loop.
  Task<Result<Stream>> Connect(std::string_view ip, std::uint16_t port);
  // Compatibility wrapper around epoll::SleepFor; loop shutdown resumes the
  // caller and discards the cancellation result. Prefer the standalone API
  // when cancellation needs to be observed.
  Task<void> SleepFor(time::Duration delay);

private:
  void RequireOwnerLoop() const noexcept;

  Loop* loop_;
  ConnectorOptions options_;
};

static_assert(backend::AsyncConnector<Connector>);

}  // namespace alyrn::epoll
