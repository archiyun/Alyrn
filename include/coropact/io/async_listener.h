// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/backend/async_listener.h"

namespace coropact::io {

template <class T>
concept AsyncListener = backend::AsyncListener<T>;

}  // namespace coropact::io
