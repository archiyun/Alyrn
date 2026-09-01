// SPDX-License-Identifier: MIT
#pragma once

// Shared adapter-contract header for epoll and uring. Include this
// file directly; there is no alyrn/backend.h. Applications use alyrn/io.h.

#include <concepts>
#include <cstdint>
#include <string_view>

#include "alyrn/backend/async_stream.h"
#include "alyrn/result.h"
#include "alyrn/task.h"

namespace alyrn::backend {

template <class T>
concept AsyncConnector = requires(T& connector, std::string_view host, std::uint16_t port) {
  typename T::StreamType;
  requires AsyncStream<typename T::StreamType>;
  { connector.Connect(host, port) } -> std::same_as<Task<Result<typename T::StreamType>>>;
};

}  // namespace alyrn::backend
