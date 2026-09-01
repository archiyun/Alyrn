// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>

#include "alyrn/net/buffer.h"
#include "alyrn/result.h"

namespace alyrn::net {

struct RecvOutcome {
  Result<std::size_t> result;
  Buffer buffer;
};

}  // namespace alyrn::net
