// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>

#include "coropact/result.h"
#include "coropact/backend/async_stream.h"
#include "coropact/coro/task.h"

namespace coropact::backend {

template <class T>
concept AsyncListener = requires(T& listener) {
  typename T::Stream;
  requires AsyncStream<typename T::Stream>;
  { listener.Accept() } -> std::same_as<
      coro::Task<Result<typename T::Stream>>>;
  { listener.Close() } -> std::same_as<coro::Task<Result<void>>>;
};

}  // namespace coropact::backend
