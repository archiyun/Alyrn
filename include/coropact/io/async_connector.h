// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#pragma once

#include <concepts>
#include <cstdint>
#include <string_view>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/io/async_stream.h"

namespace coropact::io {

template <class T>
concept AsyncConnector = requires(T& connector, std::string_view host, std::uint16_t port) {
  typename T::Stream;
  requires AsyncStream<typename T::Stream>;
  { connector.Connect(host, port) } -> std::same_as<coro::Task<base::Result<typename T::Stream>>>;
};

}  // namespace coropact::io
