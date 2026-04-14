// Copyright 2024 [Author/Organization]. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...

#pragma once

#include <cstdint>
#include <string>

namespace runtime {
namespace time {

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
  static Timestamp Now();

  // Returns an invalid timestamp (equivalent to default construction).
  //
  // Useful as a sentinel value or for initialization before assignment.
  static Timestamp Invalid();

  // Returns true if this timestamp represents a valid point in time.
  //
  // A timestamp is considered valid if its value is greater than 0.
  // Default-constructed and Invalid() timestamps return false.
  bool Valid() const;

  // Returns the time as microseconds since Unix epoch.
  std::uint64_t MicrosecondsSinceEpoch() const;

  // Returns the time as milliseconds since Unix epoch.
  //
  // Note: This truncates, not rounds, the microsecond value.
  std::uint64_t MillisecondsSinceEpoch() const;

  // Returns the time as seconds since Unix epoch.
  //
  // Note: This truncates, not rounds, the sub-second value.
  std::uint64_t SecondsSinceEpoch() const;

  // Returns a machine-readable string representation.
  //
  // Format: "seconds.microseconds" (e.g., "1234567890.123456").
  // Suitable for parsing and inter-process communication.
  std::string ToString() const;

  // Returns a human-readable formatted string.
  //
  // Args:
  //   show_microseconds: If true, includes microsecond precision.
  //
  // Format with microseconds:    "YYYY-MM-DD HH:MM:SS.uuuuuu"
  // Format without microseconds: "YYYY-MM-DD HH:MM:SS"
  //
  // The time is formatted in local timezone.
  std::string ToFormattedString(bool show_microseconds = true) const;

  // Comparison operators for ordering timestamps chronologically.
  friend bool operator<(const Timestamp& lhs, const Timestamp& rhs);
  friend bool operator>(const Timestamp& lhs, const Timestamp& rhs);
  friend bool operator==(const Timestamp& lhs, const Timestamp& rhs);
  friend bool operator>=(const Timestamp& lhs, const Timestamp& rhs);
  friend bool operator<=(const Timestamp& lhs, const Timestamp& rhs);

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
double TimeDifference(const Timestamp& high, const Timestamp& low);

// Creates a new timestamp by adding seconds to an existing timestamp.
//
// Args:
//   timestamp: The base timestamp.
//   seconds: The number of seconds to add (can be negative).
//
// Returns:
//   A new Timestamp representing the adjusted time.
Timestamp AddTime(const Timestamp& timestamp, double seconds);

}  // namespace time
}  // namespace runtime