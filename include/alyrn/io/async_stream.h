// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/backend/async_stream.h"

namespace alyrn::io {

template <class T>
concept AsyncReadStream = backend::AsyncReadStream<T>;

template <class T>
concept AsyncRecvStream = backend::AsyncRecvStream<T>;

template <class T>
concept AsyncRecvCopyStream = backend::AsyncRecvCopyStream<T>;

template <class T>
concept AsyncWriteStream = backend::AsyncWriteStream<T>;

template <class T>
concept AsyncDeadlineStream = backend::AsyncDeadlineStream<T>;

template <class T>
concept AsyncClosableStream = backend::AsyncClosableStream<T>;

template <class T>
concept AsyncStream = backend::AsyncStream<T>;

}  // namespace alyrn::io
