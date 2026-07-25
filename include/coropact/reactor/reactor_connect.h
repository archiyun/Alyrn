// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <cstdint>
#include <string_view>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/reactor/event_loop.h"
#include "coropact/net/endpoint.h"
#include "coropact/reactor/reactor_stream.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

class ReactorConnector {
public:
  COROPACT_DELETE_COPY(ReactorConnector);

  using Stream = ReactorStream;

  [[nodiscard]] static base::Result<ReactorConnector> Create(EventLoop* loop) noexcept;

  explicit ReactorConnector(EventLoop* loop) noexcept;

  ReactorConnector(ReactorConnector&& other) noexcept;
  ReactorConnector& operator=(ReactorConnector&& other) noexcept;

  coro::Task<base::Result<ReactorStream>> Connect(const net::Endpoint& peer);
  coro::Task<base::Result<ReactorStream>> Connect(std::string_view host, std::uint16_t port);
  coro::Task<void> SleepFor(std::chrono::milliseconds delay);

private:
  EventLoop* loop_;
};

}  // namespace coropact::reactor
