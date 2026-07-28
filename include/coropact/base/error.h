// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <expected>
#include <system_error>

namespace coropact::base {

using Error = std::error_code;

template <typename T>
using Result = std::expected<T, Error>;

// Converts a positive errno value to 'coropact::base::Error'.
inline Error MakeErrno(int err) noexcept { return Error{err, std::system_category()}; }

// Converts a negative errno value to 'coropact::base::Error'.
inline Error MakeNegErrno(int neg_err) noexcept { return MakeErrno(-neg_err); }

inline Error CurrentErrno() noexcept { return MakeErrno(errno); }

}  // namespace coropact::base
