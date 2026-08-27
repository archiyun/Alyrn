// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>

#include "alyrn/io/async_stream.h"
#include "alyrn/result.h"
#include "alyrn/task.h"

namespace alyrn::io {

template <class T>
concept AsyncListener = requires(T& listener) {
  typename T::StreamType;
  requires AsyncStream<typename T::StreamType>;
  { listener.Accept() } -> std::same_as<Task<Result<typename T::StreamType>>>;
  { listener.Close() } -> std::same_as<Task<Result<void>>>;
};

}  // namespace alyrn::io
