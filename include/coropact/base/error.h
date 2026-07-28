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
inline Error MakeErrno(int err) noexcept { return Error(err, std::system_category()); }

// Converts a negative errno value to 'coropact::base::Error'.
inline Error MakeNegErrno(int neg_err) noexcept { return MakeErrno(-neg_err); }

inline Error CurrentErrno() noexcept { return MakeErrno(errno); }

// Compatibility aliases for code that has not migrated to the public
// PascalCase API yet (notably the reactor module).
inline Error make_errno(int err) noexcept { return MakeErrno(err); }
inline Error make_neg_errno(int neg_err) noexcept { return MakeNegErrno(neg_err); }

}  // namespace coropact::base
