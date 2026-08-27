// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/backend/recv_source.h"

namespace alyrn::io {

using BufferLease = backend::BufferLease;
using RecvEvent = backend::RecvEvent;
using RecvSourceOptions = backend::RecvSourceOptions;

template <class T>
concept AsyncRecvSource = backend::AsyncRecvSource<T>;

}  // namespace alyrn::io
