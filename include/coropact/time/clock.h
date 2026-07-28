// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <cstdint>

namespace coropact::time {

// Returns monotonic elapsed time in whole seconds from the steady clock's
// epoch. Use this value for measuring intervals and enforcing timeouts; it has
// no calendar-time meaning.
inline std::uint64_t SteadyNow() noexcept {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count());
}

// Returns monotonic elapsed time in whole milliseconds from the steady clock's
// epoch. Use this value for measuring intervals and enforcing timeouts; it has
// no calendar-time meaning.
inline std::uint64_t SteadyNowMs() noexcept {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count());
}

// Returns monotonic elapsed time in whole nanoseconds from the steady clock's
// epoch. Use this value for measuring intervals and enforcing timeouts; it has
// no calendar-time meaning.
inline std::uint64_t SteadyNowNs() noexcept {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count());
}

}  // namespace coropact::time
