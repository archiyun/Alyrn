// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/backend/loop.h"

namespace coropact::io {

using LoopState = backend::LoopState;

template <typename T>
concept ManagedLoop = backend::ManagedLoop<T>;

}  // namespace coropact::io
