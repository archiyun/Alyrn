// Copyright 2024 [Author/Organization]. All rights reserved.
//
// Implementation of the Timestamp class for time representation and formatting.

#include "runtime/time/timestamp.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace runtime {
namespace time {

// static
Timestamp Timestamp::Now() {
  // std::chrono::system_clock provides wall-clock time that can be
  // converted to calendar time. We use duration_cast to truncate
  // (not round) to microsecond precision.
  const auto now = std::chrono::system_clock::now();
  const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
      now.time_since_epoch());
  return Timestamp(static_cast<std::uint64_t>(micros.count()));
}

// static
Timestamp Timestamp::Invalid() {
  // Default construction yields microseconds_since_epoch_ = 0,
  // which is treated as invalid by Valid().
  return Timestamp();
}

bool Timestamp::Valid() const {
  // Unix epoch (1970-01-01 00:00:00 UTC) itself is treated as invalid.
  // This is acceptable since we rarely need to represent that exact moment.
  return microseconds_since_epoch_ > 0;
}

std::uint64_t Timestamp::MicrosecondsSinceEpoch() const {
  return microseconds_since_epoch_;
}

std::uint64_t Timestamp::MillisecondsSinceEpoch() const {
  // Integer division truncates toward zero.
  return microseconds_since_epoch_ / 1000;
}

std::uint64_t Timestamp::SecondsSinceEpoch() const {
  // Integer division truncates toward zero.
  return microseconds_since_epoch_ / 1'000'000;
}

std::string Timestamp::ToString() const {
  // Format: "seconds.microseconds" (e.g., "1713100800.000123")
  // The microsecond part is zero-padded to 6 digits for consistent parsing.
  std::ostringstream oss;
  oss << SecondsSinceEpoch() << '.'
      << std::setw(6) << std::setfill('0')
      << (microseconds_since_epoch_ % 1'000'000);
  return oss.str();
}

std::string Timestamp::ToFormattedString(bool show_microseconds) const {
  const std::time_t seconds = static_cast<std::time_t>(SecondsSinceEpoch());

  // Use thread-safe localtime variants to avoid data races when multiple
  // threads format timestamps concurrently. The standard localtime() uses
  // a static buffer that would cause undefined behavior.
  std::tm tm_time{};
#if defined(_WIN32)
  localtime_s(&tm_time, &seconds);  // Windows: dst, src (reversed order)
#else
  localtime_r(&seconds, &tm_time);  // POSIX: src, dst
#endif

  // Format: "YYYY-MM-DD HH:MM:SS[.uuuuuu]"
  // %F = %Y-%m-%d (ISO 8601 date)
  // %T = %H:%M:%S (24-hour time)
  std::ostringstream oss;
  oss << std::put_time(&tm_time, "%F %T");

  if (show_microseconds) {
    oss << '.'
        << std::setw(6) << std::setfill('0')
        << (microseconds_since_epoch_ % 1'000'000);
  }
  return oss.str();
}

bool operator<(const Timestamp& lhs, const Timestamp& rhs) {
  return lhs.microseconds_since_epoch_ < rhs.microseconds_since_epoch_;
}

bool operator>(const Timestamp& lhs, const Timestamp& rhs) {
  return lhs.microseconds_since_epoch_ > rhs.microseconds_since_epoch_;
}

bool operator==(const Timestamp& lhs, const Timestamp& rhs) {
  return lhs.microseconds_since_epoch_ == rhs.microseconds_since_epoch_;
}

bool operator>=(const Timestamp& lhs, const Timestamp& rhs) {
  return lhs.microseconds_since_epoch_ >= rhs.microseconds_since_epoch_;
}

bool operator<=(const Timestamp& lhs, const Timestamp& rhs) {
  return lhs.microseconds_since_epoch_ <= rhs.microseconds_since_epoch_;
}
double TimeDifference(const Timestamp& high, const Timestamp& low) {
  // Convert to double before subtraction to handle cases where
  // low > high (result would underflow with unsigned arithmetic).
  // The result can be negative, which is intentional.
  const double delta =
      static_cast<double>(high.MicrosecondsSinceEpoch()) -
      static_cast<double>(low.MicrosecondsSinceEpoch());
  return delta / 1'000'000.0;
}

// Adds seconds (can be fractional) to a timestamp safely.
// Returns Invalid() if the addition would underflow.
Timestamp AddTime(const Timestamp& timestamp, double seconds) {
  const auto micros = timestamp.MicrosecondsSinceEpoch();
  const auto delta = static_cast<std::int64_t>(seconds * 1'000'000.0);

  if (delta < 0 && static_cast<std::uint64_t>(-delta) > micros) {
    return Timestamp::Invalid();
  }

  return Timestamp(micros + delta);
}

}  // namespace time
}  // namespace runtime
