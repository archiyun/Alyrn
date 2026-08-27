// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/detail/backend/loop.h"

namespace alyrn::io {

using LoopState = ::alyrn::detail::backend::LoopState;

template <typename T>
concept ManagedLoop = ::alyrn::detail::backend::ManagedLoop<T>;

}  // namespace alyrn::io
