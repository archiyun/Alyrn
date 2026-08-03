// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>

#include "coropact/base/error.h"
#include "coropact/net/buffer.h"

namespace coropact::net {

struct ReadIntoOutcome {
  base::Result<std::size_t> result;
  Buffer buffer;
};

}  // namespace coropact::net
