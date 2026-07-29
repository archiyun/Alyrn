// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <compare>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>

namespace coropact::time {

// Represents a specific point in time with microsecond precision.
//
// Timestamp provides a lightweight, immutable representation of a moment
// in time, measured as microseconds since the Unix epoch (1970-01-01 00:00:00
// UTC). It is designed for:
//   - Logging and diagnostics
//   - Performance measurement and statistics
//   - Timer scheduling (as a building block)
//
// This class focuses solely on time representation and formatting. Scheduling
// logic should be implemented separately (e.g., in a Timer class).
//
// Example usage:
//   Timestamp now = Timestamp::Now();
//   LOG(INFO) << "Current time: " << now.ToFormattedString();
//
//   Timestamp later = AddTime(now, 5.0);  // 5 seconds later
//   double elapsed = TimeDifference(later, now);  // 5.0
//
// Thread-safety: Timestamp objects are immutable after construction and
// are safe to use concurrently from multiple threads.
class Timestamp {
 public:
  // Constructs an invalid timestamp (epoch time 0).
  Timestamp() = default;

  // Constructs a timestamp from the given microseconds since Unix epoch.
  //
  // Args:
  //   microseconds_since_epoch: The number of microseconds elapsed since
  //       1970-01-01 00:00:00 UTC. Must be non-negative.
  explicit Timestamp(std::uint64_t microseconds_since_epoch)
      : microseconds_since_epoch_(microseconds_since_epoch) {}

  // Returns the current system time as a Timestamp.
  //
  // Uses the system's real-time clock. The precision depends on the
  // underlying platform but is typically microsecond-level.
  static Timestamp Now() {
    const auto now = std::chrono::system_clock::now();
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch());
    return Timestamp(static_cast<std::uint64_t>(micros.count()));
  }

  // Returns an invalid timestamp (equivalent to default construction).
  //
  // Useful as a sentinel value or for initialization before assignment.
  static Timestamp Invalid() { return Timestamp(); }

  // Returns true if this timestamp represents a valid point in time.
  //
  // A timestamp is considered valid if its value is greater than 0.
  // Default-constructed and Invalid() timestamps return false.
  bool Valid() const { return microseconds_since_epoch_ > 0; }

  // Returns the time as microseconds since Unix epoch.
  std::uint64_t MicrosecondsSinceEpoch() const { return microseconds_since_epoch_; }

  // Returns the time as milliseconds since Unix epoch.
  //
  // Note: This truncates, not rounds, the microsecond value.
  std::uint64_t MillisecondsSinceEpoch() const { return microseconds_since_epoch_ / 1000; }

  // Returns the time as seconds since Unix epoch.
  //
  // Note: This truncates, not rounds, the sub-second value.
  std::uint64_t SecondsSinceEpoch() const { return microseconds_since_epoch_ / 1'000'000; }

  // Returns a machine-readable string representation.
  //
  // Format: "seconds.microseconds" (e.g., "1234567890.123456").
  // Suitable for parsing and inter-process communication.
  std::string ToString() const {
    std::ostringstream oss;
    oss << SecondsSinceEpoch() << '.' << std::setw(6) << std::setfill('0')
        << (microseconds_since_epoch_ % 1'000'000);
    return oss.str();
  }

  // Returns a human-readable formatted string.
  //
  // Args:
  //   show_microseconds: If true, includes microsecond precision.
  //
  // Format with microseconds:    "YYYY-MM-DD HH:MM:SS.uuuuuu"
  // Format without microseconds: "YYYY-MM-DD HH:MM:SS"
  //
  // The time is formatted in local timezone.
  std::string ToFormattedString(bool show_microseconds = true) const {
    const std::time_t seconds = static_cast<std::time_t>(SecondsSinceEpoch());
    std::tm tm_time{};
#if defined(_WIN32)
    localtime_s(&tm_time, &seconds);
#else
    localtime_r(&seconds, &tm_time);
#endif

    std::ostringstream oss;
    oss << std::put_time(&tm_time, "%F %T");
    if (show_microseconds) {
      oss << '.' << std::setw(6) << std::setfill('0')
          << (microseconds_since_epoch_ % 1'000'000);
    }
    return oss.str();
  }
  
  // support operator {>= <= == > <  != } in C++23
  auto operator<=>(const Timestamp& rhs) const noexcept -> std::strong_ordering {
    const Timestamp& lhs = *this;
    return lhs.microseconds_since_epoch_ <=> rhs.microseconds_since_epoch_;
  }
private:
  // Microseconds since 1970-01-01 00:00:00 UTC.
  // A value of 0 indicates an invalid/uninitialized timestamp.
  std::uint64_t microseconds_since_epoch_{0};
};

// Returns the time difference between two timestamps in seconds.
//
// Args:
//   high: The later timestamp.
//   low: The earlier timestamp.
//
// Returns:
//   The difference (high - low) in seconds as a floating-point value.
//   May be negative if `low` is actually later than `high`.
inline double TimeDifference(const Timestamp& high, const Timestamp& low) {
  const double delta = static_cast<double>(high.MicrosecondsSinceEpoch()) -
                       static_cast<double>(low.MicrosecondsSinceEpoch());
  return delta / 1'000'000.0;
}

// Creates a new timestamp by adding seconds to an existing timestamp.
//
// Args:
//   timestamp: The base timestamp.
//   seconds: The number of seconds to add (can be negative).
//
// Returns:
//   A new Timestamp representing the adjusted time.
inline Timestamp AddTime(const Timestamp& timestamp, double seconds) {
  const auto micros = timestamp.MicrosecondsSinceEpoch();
  const auto delta = static_cast<std::int64_t>(seconds * 1'000'000.0);
  if (delta < 0 && static_cast<std::uint64_t>(-delta) > micros) {
    return Timestamp::Invalid();
  }
  return Timestamp(micros + delta);
}

}  // namespace coropact::time
