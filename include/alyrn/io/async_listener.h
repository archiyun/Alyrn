// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/backend/async_listener.h"

namespace alyrn::io {

template <class T>
concept AsyncListener = backend::AsyncListener<T>;

}  // namespace alyrn::io
