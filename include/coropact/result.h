// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <expected>
#include <system_error>

namespace coropact {

// Default error model used by fallible CoroPact operations.
using Error = std::error_code;

// A fallible result. CoroPact APIs use the default Error type; adapters may
// select a domain-specific error type without introducing a second result
// abstraction.
template <typename T, typename E = Error>
using Result = std::expected<T, E>;

// Converts a positive errno value to Error.
[[nodiscard]]
inline Error Errno(int value) noexcept {
  return {value, std::system_category()};
}

// Converts a negative errno value, such as an io_uring CQE result, to Error.
[[nodiscard]]
inline Error NegErrno(int value) noexcept {
  return Errno(-value);
}

[[nodiscard]]
inline Error CurrentErrno() noexcept {
  return Errno(errno);
}

}  // namespace coropact
