// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/backend/async_stream.h"

namespace coropact::io {

template <class T>
concept AsyncReadStream = backend::AsyncReadStream<T>;

template <class T>
concept AsyncTimedReadStream = backend::AsyncTimedReadStream<T>;

template <class T>
concept AsyncReadIntoStream = backend::AsyncReadIntoStream<T>;

template <class T>
concept AsyncWriteStream = backend::AsyncWriteStream<T>;

template <class T>
concept AsyncClosableStream = backend::AsyncClosableStream<T>;

template <class T>
concept AsyncStream = backend::AsyncStream<T>;

template <class T>
concept AsyncTimedStream = backend::AsyncTimedStream<T>;

}  // namespace coropact::io
