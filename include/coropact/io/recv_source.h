// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/backend/recv_source.h"

namespace coropact::io {

using BufferLease = backend::BufferLease;
using RecvEvent = backend::RecvEvent;
using RecvSourceOptions = backend::RecvSourceOptions;

template <class T>
concept AsyncRecvSource = backend::AsyncRecvSource<T>;

}  // namespace coropact::io
