#include <chrono>
#include <iostream>

#include "alyrn/time/clock.h"

int main() {
  using namespace alyrn::time;
  static_assert(noexcept(SteadyNow()));

  const Deadline first = SteadyNow();
  const Deadline second = SteadyNow();
  if (second < first) {
    std::cerr << "[FAIL] steady clock moved backwards\n";
    return 1;
  }

  const Duration combined = Seconds(1) + Milliseconds(250) + Microseconds(3) + Nanoseconds(4);
  const auto expected = std::chrono::seconds(1) + std::chrono::milliseconds(250) +
                        std::chrono::microseconds(3) + std::chrono::nanoseconds(4);
  if (combined != std::chrono::duration_cast<Duration>(expected)) {
    std::cerr << "[FAIL] duration constructors produced an unexpected value\n";
    return 1;
  }

  std::cout << "[PASS] clock_smoke_test\n";
  return 0;
}
