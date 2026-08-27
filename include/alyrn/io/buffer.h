// SPDX-License-Identifier: MIT
#pragma once

// Public spelling for the backend-neutral byte store. The implementation
// remains in net because Reactor and luring use it below the io facade; this
// alias deliberately adds neither allocation nor a backend dependency.
#include "alyrn/net/buffer.h"

namespace alyrn::io {

using Buffer = ::alyrn::net::Buffer;

}  // namespace alyrn::io
