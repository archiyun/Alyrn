// SPDX-License-Identifier: MIT
#pragma once

// Shared adapter-contract header for epoll and uring. Include this
// file directly; there is no alyrn/backend.h. Applications use alyrn/io.h.

#include <concepts>

#include "alyrn/backend/async_stream.h"
#include "alyrn/result.h"
#include "alyrn/task.h"

namespace alyrn::backend {

template <class T>
concept AsyncListener = requires(T& listener) {
  typename T::StreamType;
  requires AsyncStream<typename T::StreamType>;
  { listener.Accept() } -> std::same_as<Task<Result<typename T::StreamType>>>;
  { listener.Close() } -> std::same_as<Task<Result<void>>>;
};

}  // namespace alyrn::backend
