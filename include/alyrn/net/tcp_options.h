// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <optional>

#include "alyrn/time/clock.h"

namespace alyrn::net {

// Unset members keep the operating-system default. Every member has a default
// initializer so designated initialization such as TcpOptions{.no_delay = true}
// stays clean under -Wextra (-Wmissing-field-initializers).
struct TcpOptions {
  std::optional<bool> no_delay{};
  std::optional<bool> keep_alive{};
  std::optional<time::Duration> keep_alive_period{};
  std::optional<std::size_t> read_buffer{};
  std::optional<std::size_t> write_buffer{};
};

}  // namespace alyrn::net
