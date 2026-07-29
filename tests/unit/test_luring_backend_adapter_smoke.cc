// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <cerrno>

#include <iostream>
#include <system_error>

#include "coropact/io/luring_backend.h"

namespace {

bool IsEnvironmentSkip(const coropact::base::Error& error) {
  return error == std::errc::operation_not_supported ||
         error == std::errc::operation_not_permitted ||
         error == std::errc::permission_denied ||
         error == std::errc::function_not_supported ||
         error.value() == EINVAL;
}

}  // namespace

int main() {
  coropact::luring::LUringOptions options;
  options.entries = 16;

  auto binding = coropact::io::BindLuring(
      options, coropact::io::CapabilitySet::CoreGateway());
  if (!binding.has_value()) {
    if (IsEnvironmentSkip(binding.error())) {
      std::cout << "SKIP: io_uring adapter unavailable: "
                << binding.error().message() << '\n';
      return 0;
    }
    std::cout << "FAIL: io_uring adapter binding failed: "
              << binding.error().message() << '\n';
    return 1;
  }

  if (!binding->active_profile.ContainsAll(
          coropact::luring::RuntimeProfile::Core())) {
    std::cout << "FAIL: adapter did not select the native core profile\n";
    return 1;
  }
  if (!options.active_profile.ContainsAll(binding->active_profile)) {
    std::cout << "FAIL: adapter did not install the selected profile\n";
    return 1;
  }

  std::cout << "luring backend adapter smoke: PASS\n";
  return 0;
}
