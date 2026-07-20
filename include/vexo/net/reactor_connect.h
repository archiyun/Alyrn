// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string_view>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/reactor_stream.h"
#include "vexo/utils/macros.h"

namespace vexo::net {

class ReactorConnector {
public:
  VEXO_DELETE_COPY_MOVE(ReactorConnector);

  using Stream = ReactorStream;

  explicit ReactorConnector(EventLoop* loop) noexcept : loop_(loop) {}

  coro::Task<base::Result<std::unique_ptr<ReactorStream>>> Connect(std::string_view host,
                                                                   std::uint16_t port);
  coro::Task<void> SleepFor(std::chrono::milliseconds delay);

private:
  EventLoop* loop_;
};

}  // namespace vexo::net
