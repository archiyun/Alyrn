// SPDX-License-Identifier: MIT
#pragma once

// Shared adapter-contract header for epoll and uring. Include this
// file directly; there is no alyrn/backend.h. Applications use alyrn/io.h.

#include <concepts>
#include <cstdint>
#include <string_view>

#include "alyrn/backend/async_stream.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"
#include "alyrn/task.h"

namespace alyrn::backend {

// Connect(ip, port) takes a numeric IPv4/IPv6 literal. Name resolution is not
// part of the connector contract; a non-numeric address completes with EINVAL.
// Connect(endpoint) takes an address resolved elsewhere, for example by
// getaddrinfo on a helper thread.
template <class T>
concept AsyncConnector =
    requires(T& connector, std::string_view ip, std::uint16_t port, net::Endpoint endpoint) {
      typename T::StreamType;
      requires AsyncStream<typename T::StreamType>;
      { connector.Connect(ip, port) } -> std::same_as<Task<Result<typename T::StreamType>>>;
      { connector.Connect(endpoint) } -> std::same_as<Task<Result<typename T::StreamType>>>;
    };

}  // namespace alyrn::backend
