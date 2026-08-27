// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/io/async_listener.h"

namespace alyrn::detail::backend {

template <class T>
concept AsyncListener = io::AsyncListener<T>;

}  // namespace alyrn::detail::backend
