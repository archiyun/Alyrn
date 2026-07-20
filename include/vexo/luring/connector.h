// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string_view>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/luring/loop.h"
#include "vexo/luring/stream.h"
#include "vexo/utils/macros.h"

namespace vexo::luring {

class LUringConnector {
public:
  VEXO_DELETE_COPY(LUringConnector);

  LUringConnector(LUringConnector&&) noexcept = default;
  LUringConnector& operator=(LUringConnector&&) noexcept = default;

  using Stream = LUringStream;

  explicit LUringConnector(LUringLoop* loop) noexcept : loop_(loop) {}

  coro::Task<base::Result<std::unique_ptr<LUringStream>>> Connect(std::string_view host,
                                                                  std::uint16_t port);

  // Backend-selected timer used by the generic gateway health-check loop.
  coro::Task<void> SleepFor(std::chrono::milliseconds delay);

private:
  LUringLoop* loop_;
};

}  // namespace vexo::luring
