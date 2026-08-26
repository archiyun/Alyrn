// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <string_view>

#include "coropact/result.h"
#include "coropact/coro/task.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/stream.h"
#include "coropact/time/clock.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

class Connector {
public:
  COROPACT_DELETE_COPY(Connector);

  using StreamType = Stream;

  Connector(Connector&&) noexcept;
  Connector& operator=(Connector&&) noexcept;

  [[nodiscard]]
  static Result<Connector> Create(Loop* loop) noexcept;
  explicit Connector(Loop* loop) noexcept;

  // Connect is loop-affine. Independent calls may be pending concurrently;
  // each call owns its socket, physical request, result, and continuation.
  coro::Task<Result<Stream>> Connect(std::string_view host, std::uint16_t port);

  // Backend-selected timer for application-level health-check loops.
  coro::Task<void> SleepFor(time::Duration delay);

private:
  void RequireOwnerLoop() const noexcept;

  Loop* loop_;
};

}  // namespace coropact::luring
