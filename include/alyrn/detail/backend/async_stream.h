// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/io/async_stream.h"

namespace alyrn::detail::backend {

template <class T>
concept AsyncReadStream = io::AsyncReadStream<T>;

template <class T>
concept AsyncReadIntoStream = io::AsyncReadIntoStream<T>;

template <class T>
concept AsyncWriteStream = io::AsyncWriteStream<T>;

template <class T>
concept AsyncClosableStream = io::AsyncClosableStream<T>;

template <class T>
concept AsyncStream = io::AsyncStream<T>;

}  // namespace alyrn::detail::backend
