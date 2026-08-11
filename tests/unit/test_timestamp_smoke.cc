// Copyright (c) 2026 Arsenova.
// SPDX-License-Identifier: MIT

#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>

#include "coropact/time/timestamp.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

bool TestAddTime() {
  using coropact::time::AddTime;
  using coropact::time::Timestamp;

  const Timestamp base(2'000'000);
  const Timestamp forward = AddTime(base, 1.25);
  const Timestamp backward = AddTime(base, -0.5);

  return Expect(forward.MicrosecondsSinceEpoch() == 3'250'000,
                "positive AddTime must preserve microsecond precision") &&
         Expect(backward.MicrosecondsSinceEpoch() == 1'500'000,
                "negative AddTime must subtract microseconds") &&
         Expect(!AddTime(Timestamp(5), -0.000006).Valid(),
                "negative underflow must return Invalid") &&
         Expect(!AddTime(base, std::numeric_limits<double>::infinity()).Valid(),
                "infinite AddTime must return Invalid") &&
         Expect(!AddTime(base, std::numeric_limits<double>::quiet_NaN()).Valid(),
                "NaN AddTime must return Invalid") &&
         Expect(!AddTime(base, std::numeric_limits<double>::max()).Valid(),
                "out-of-range finite AddTime must return Invalid");
}

bool TestAddTimeOverflow() {
  using coropact::time::AddTime;
  using coropact::time::Timestamp;

  constexpr auto kMax = std::numeric_limits<std::uint64_t>::max();
  const Timestamp near_max(kMax - 100);
  const Timestamp within_range = AddTime(near_max, 0.000050);

  return Expect(within_range.MicrosecondsSinceEpoch() == kMax - 50,
                "AddTime must retain an in-range positive delta") &&
         Expect(!AddTime(near_max, 0.000101).Valid(),
                "positive overflow must return Invalid rather than wrap");
}

bool TestTimeDifferenceAtUint64Boundary() {
  using coropact::time::TimeDifference;
  using coropact::time::Timestamp;

  constexpr auto kMax = std::numeric_limits<std::uint64_t>::max();
  return Expect(TimeDifference(Timestamp(kMax), Timestamp(kMax - 1)) == 0.000001,
                "TimeDifference must subtract before converting to double") &&
         Expect(TimeDifference(Timestamp(kMax - 1), Timestamp(kMax)) == -0.000001,
                "TimeDifference must preserve negative one-microsecond deltas");
}

}  // namespace

int main() {
  if (!TestAddTime()) return 1;
  if (!TestAddTimeOverflow()) return 1;
  if (!TestTimeDifferenceAtUint64Boundary()) return 1;

  std::cout << "[PASS] timestamp_smoke_test\n";
  return 0;
}
