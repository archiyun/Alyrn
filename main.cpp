#include <print>

auto main() -> int {
  std::println(R"logo(
  ____                ____            _
 / ___|___  _ __ ___ |  _ \ __ _  ___| |_
| |   / _ \| '__/ _ \| |_) / _` |/ __| __|
| |__| (_) | | | (_) |  __/ (_| | (__| |_
 \____\___/|_|  \___/|_|   \__,_|\___|\__|
)logo");

  std::println("Alyrn is working now.");
  std::println("Welcome to Alyrn.");
  return 0;
}
