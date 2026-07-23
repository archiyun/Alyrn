// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/io/async_stream.h"

namespace coropact::io {

template <class T>
concept AsyncListener = requires(T& listener) {
  typename T::Stream;
  requires AsyncStream<typename T::Stream>;
  { listener.Accept() } -> std::same_as<coro::Task<base::Result<typename T::Stream>>>;
  { listener.Close() } -> std::same_as<coro::Task<base::Result<void>>>;
};

}  // namespace coropact::io
