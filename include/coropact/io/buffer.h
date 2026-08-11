// SPDX-License-Identifier: MIT
#pragma once

// Public spelling for the backend-neutral byte store. The implementation
// remains in net because Reactor and luring use it below the io facade; this
// alias deliberately adds neither allocation nor a backend dependency.
#include "coropact/net/buffer.h"

namespace coropact::io {

using Buffer = ::coropact::net::Buffer;

}  // namespace coropact::io
