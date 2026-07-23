#include <print>

// test C++23 environments
auto main(int argc, char** argv) -> int {
  std::println(R"logo(
__     __
\ \   / /__ __  __ ___
 \ \ / / _ \\ \/ // _ \
  \ V /  __/>  <| (_) |
   \_/ \___/_/\_\\___/
)logo");
  std::println("C++23 is working now.");
  std::println("Welcome to CoroPact.");
  return 0;
}
