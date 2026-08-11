// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>

#include "coropact/result.h"
#include "coropact/net/buffer.h"

namespace coropact::net {

struct ReadIntoOutcome {
  Result<std::size_t> result;
  Buffer buffer;
};

}  // namespace coropact::net
