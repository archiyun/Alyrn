// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>

#include "alyrn/result.h"
#include "alyrn/backend/async_stream.h"
#include "alyrn/coro/task.h"

namespace alyrn::backend {

template <class T>
concept AsyncListener = requires(T& listener) {
  typename T::StreamType;
  requires AsyncStream<typename T::StreamType>;
  { listener.Accept() } -> std::same_as<
      coro::Task<Result<typename T::StreamType>>>;
  { listener.Close() } -> std::same_as<coro::Task<Result<void>>>;
};

}  // namespace alyrn::backend
