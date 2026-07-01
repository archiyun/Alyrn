// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <memory>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/net/async_stream.h"

namespace vexo::net {

template <class T>
concept AsyncListener = requires(T& listener) {
  typename T::Stream;
  requires AsyncStream<typename T::Stream>;
  {
    listener.Accept()
  } -> std::same_as<coro::Task<base::Result<std::unique_ptr<typename T::Stream>>>>;
  { listener.Close() } -> std::same_as<coro::Task<base::Result<void>>>;
};

}  // namespace vexo::net
