// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

// Public spelling for the backend-neutral segmented byte store. The type is
// implemented in net because Reactor and luring use it below the io facade;
// this alias deliberately adds neither allocation nor a backend dependency.
#include "coropact/net/segmented_buffer.h"

namespace coropact::io {

using Buffer = ::coropact::net::SegmentedBuffer;

}  // namespace coropact::io
