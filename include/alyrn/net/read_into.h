// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>

#include "alyrn/result.h"
#include "alyrn/net/buffer.h"

namespace alyrn::net {

struct ReadIntoOutcome {
  Result<std::size_t> result;
  Buffer buffer;
};

}  // namespace alyrn::net
