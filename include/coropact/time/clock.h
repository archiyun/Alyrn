// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <cstdint>

namespace coropact::time {

// Returns elapsed milliseconds from the steady clock's epoch.
//
// This value is suitable for measuring intervals and enforcing timeouts. It
// must not be converted to a calendar time; use Timestamp::Now() for wall
// clock timestamps instead.
inline std::uint64_t SteadyNowMs() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

}  // namespace coropact::time
