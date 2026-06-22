// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <expected>
#include <system_error>

namespace vexo::base {

using Error = std::system_error;

template <typename T>
using Result = std::expected<T, Error>;

// Converts a positive errno value to 'vexo::base::Error'
inline Error make_errno(int err) noexcept {
  return std::error_code{err, std::system_category()};
}

// Converts a negative errno value to 'vexo::base::Error'
inline Error make_neg_errno(int neg_err) noexcept {
  return make_errno(-neg_err);
}

}  // namespace vexo::base
