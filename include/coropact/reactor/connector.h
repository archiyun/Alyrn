// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <string_view>

#include "coropact/result.h"
#include "coropact/coro/task.h"
#include "coropact/net/endpoint.h"
#include "coropact/reactor/loop.h"
#include "coropact/reactor/stream.h"
#include "coropact/time/clock.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

struct ReactorConnectorOptions {
  // Applies to every ReactorStream returned by Connect.
  ReactorStreamOptions stream_options{};
};

class ReactorConnector {
public:
  COROPACT_DELETE_COPY(ReactorConnector);

  using Stream = ReactorStream;

  [[nodiscard]]
  static Result<ReactorConnector> Create(EventLoop* loop,
                                               ReactorConnectorOptions options = {}) noexcept;

  explicit ReactorConnector(EventLoop* loop, ReactorConnectorOptions options = {}) noexcept;

  ReactorConnector(ReactorConnector&& other) noexcept;
  ReactorConnector& operator=(ReactorConnector&& other) noexcept;

  // Connect is loop-affine. Independent calls may be pending concurrently;
  // each call owns its socket, Channel, result, and continuation.
  coro::Task<Result<ReactorStream>> Connect(net::Endpoint peer);
  coro::Task<Result<ReactorStream>> Connect(std::string_view host, std::uint16_t port);
  coro::Task<void> SleepFor(time::Duration delay);

private:
  void RequireOwnerLoop() const noexcept;

  EventLoop* loop_;
  ReactorConnectorOptions options_;
};

}  // namespace coropact::reactor
