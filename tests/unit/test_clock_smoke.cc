#include <cstdint>
#include <iostream>

#include "coropact/time/clock.h"

int main() {
  static_assert(noexcept(coropact::time::SteadyNowMs()));

  const std::uint64_t first = coropact::time::SteadyNowMs();
  const std::uint64_t second = coropact::time::SteadyNowMs();
  if (second < first) {
    std::cerr << "[FAIL] steady clock moved backwards\n";
    return 1;
  }

  std::cout << "[PASS] clock_smoke_test\n";
  return 0;
}
