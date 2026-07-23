// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <cstdint>
#include <string_view>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/stream.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

class LUringConnector {
public:
  COROPACT_DELETE_COPY(LUringConnector);

  using Stream = LUringStream;

  LUringConnector(LUringConnector&&) noexcept;
  LUringConnector& operator=(LUringConnector&&) noexcept;

  [[nodiscard]] static base::Result<LUringConnector> Create(LUringLoop* loop) noexcept;
  explicit LUringConnector(LUringLoop* loop) noexcept;

  coro::Task<base::Result<LUringStream>> Connect(std::string_view host, std::uint16_t port);

  // Backend-selected timer used by the generic gateway health-check loop.
  coro::Task<void> SleepFor(std::chrono::milliseconds delay);

private:
  LUringLoop* loop_;
};

}  // namespace coropact::luring
