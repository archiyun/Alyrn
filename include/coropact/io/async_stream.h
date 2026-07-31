// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/backend/async_stream.h"

namespace coropact::io {

template <class T>
concept AsyncReadStream = backend::AsyncReadStream<T>;

template <class T>
concept AsyncOwnedReadStream = backend::AsyncOwnedReadStream<T>;

template <class T>
concept AsyncWriteStream = backend::AsyncWriteStream<T>;

template <class T>
concept AsyncClosableStream = backend::AsyncClosableStream<T>;

template <class T>
concept AsyncStream = backend::AsyncStream<T>;

using WritePart = backend::WritePart;

template <class T>
concept AsyncScatterWriteStream = backend::AsyncScatterWriteStream<T>;

}  // namespace coropact::io
