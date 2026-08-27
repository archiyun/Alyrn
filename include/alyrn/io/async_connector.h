// SPDX-License-Identifier: MIT

#pragma once

#include <concepts>
#include <cstdint>
#include <string_view>

#include "alyrn/result.h"
#include "alyrn/coro/task.h"
#include "alyrn/io/async_stream.h"

namespace alyrn::io {

// Core outbound-connection seam. Each Connect call owns an independent
// operation lifecycle and returns a Stream bound to the connector's owner
// loop. Concrete backends preserve transport errors and reject new work once
// that loop begins stopping.
template <class T>
concept AsyncConnector = requires(T& connector, std::string_view host, std::uint16_t port) {
  typename T::StreamType;
  requires AsyncStream<typename T::StreamType>;
  { connector.Connect(host, port) } -> std::same_as<coro::Task<Result<typename T::StreamType>>>;
};

}  // namespace alyrn::io
