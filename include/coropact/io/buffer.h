// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

// Compatibility spelling for the buffer primitive. The implementation lives
// below the backend facade so Reactor and io_uring do not depend on io.
#include "coropact/net/buffer.h"

namespace coropact::io {

using Buffer = ::coropact::net::Buffer;

}  // namespace coropact::io
