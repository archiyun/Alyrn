// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <cstdint>

namespace coropact::time {

// CoroPact runtime deadlines always use the monotonic clock. Calendar time is
// deliberately outside this module: wall-clock adjustments must not alter an
// I/O timeout, a repeating timer, or an operation deadline.
using Clock = std::chrono::steady_clock;
using Duration = Clock::duration;
using Deadline = Clock::time_point;

[[nodiscard]]
constexpr Duration Nanoseconds(std::int64_t value) noexcept {
  return std::chrono::duration_cast<Duration>(std::chrono::nanoseconds{value});
}

[[nodiscard]]
constexpr Duration Microseconds(std::int64_t value) noexcept {
  return std::chrono::duration_cast<Duration>(std::chrono::microseconds{value});
}

[[nodiscard]]
constexpr Duration Milliseconds(std::int64_t value) noexcept {
  return std::chrono::duration_cast<Duration>(std::chrono::milliseconds{value});
}

[[nodiscard]]
constexpr Duration Seconds(std::int64_t value) noexcept {
  return std::chrono::duration_cast<Duration>(std::chrono::seconds{value});
}

[[nodiscard]]
inline Deadline SteadyNow() noexcept {
  return Clock::now();
}

// Returns monotonic elapsed time in whole nanoseconds from the steady clock's
// epoch. Use this value for measuring intervals and enforcing timeouts; it has
// no calendar-time meaning.
inline std::uint64_t SteadyNowNs() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(SteadyNow().time_since_epoch()).count());
}

}  // namespace coropact::time
