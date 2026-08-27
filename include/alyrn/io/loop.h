// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/backend/loop.h"

namespace alyrn::io {

using LoopState = backend::LoopState;

template <typename T>
concept ManagedLoop = backend::ManagedLoop<T>;

}  // namespace alyrn::io
