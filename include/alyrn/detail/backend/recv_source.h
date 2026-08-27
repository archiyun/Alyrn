// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/io/recv_source.h"

namespace alyrn::detail::backend {

using BufferLease = io::BufferLease;
using RecvEvent = io::RecvEvent;
using RecvSourceOptions = io::RecvSourceOptions;

template <class T>
concept AsyncRecvSource = io::AsyncRecvSource<T>;

}  // namespace alyrn::detail::backend
