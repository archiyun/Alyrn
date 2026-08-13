// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <string_view>

#include "coropact/result.h"
#include "coropact/coro/task.h"
#include "coropact/net/endpoint.h"
#include "coropact/kqueue/loop.h"
#include "coropact/kqueue/stream.h"
#include "coropact/time/clock.h"
#include "coropact/utils/macros.h"

namespace coropact::kqueue {

struct KqueueConnectorOptions {
  // Applies to every KqueueStream returned by Connect.
  KqueueStreamOptions stream_options{};
};

class KqueueConnector {
public:
  COROPACT_DELETE_COPY(KqueueConnector);

  using Stream = KqueueStream;

  [[nodiscard]]
  static Result<KqueueConnector> Create(KqueueLoop* loop,
                                               KqueueConnectorOptions options = {}) noexcept;

  explicit KqueueConnector(KqueueLoop* loop, KqueueConnectorOptions options = {}) noexcept;

  KqueueConnector(KqueueConnector&& other) noexcept;
  KqueueConnector& operator=(KqueueConnector&& other) noexcept;

  // Connect is loop-affine. Independent calls may be pending concurrently;
  // each call owns its socket, Channel, result, and continuation.
  coro::Task<Result<KqueueStream>> Connect(net::Endpoint peer);
  coro::Task<Result<KqueueStream>> Connect(std::string_view host, std::uint16_t port);
  coro::Task<void> SleepFor(time::Duration delay);

private:
  void RequireOwnerLoop() const noexcept;

  KqueueLoop* loop_;
  KqueueConnectorOptions options_;
};

}  // namespace coropact::kqueue
