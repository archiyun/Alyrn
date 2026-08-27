// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <optional>

#include "coropact/time/clock.h"

namespace coropact::net {

struct TcpOptions {
  std::optional<bool> no_delay;
  std::optional<bool> keep_alive;
  std::optional<time::Duration> keep_alive_period;
  std::optional<std::size_t> read_buffer;
  std::optional<std::size_t> write_buffer;
};

}  // namespace coropact::net
