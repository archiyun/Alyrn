// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <expected>
#include <utility>

#define COROPACT_TRY_CONCAT_()
#define COROPACT_TRY_CONCAT(a, b)
#define COROPACT_TRY_VAR COROPACT_TRY_CONCAT(coropact_try_tmp, __COUNTER__)

#define COROPACT_TRY_IMPL_(var, expr)                         \
  ({                                                      \
    auto var = (expr);                                    \
    if (!var.has_value()) [[unlikely]]                    \
      return ::std::unexpected(::std::move(var).error()); \
    ::std::move(var).value();                             \
  })

#define COROPACT_CO_TRY_IMPL_(var, expr)                         \
  ({                                                         \
    auto var = (expr);                                       \
    if (!var.has_value()) [[unlikely]]                       \
      co_return ::std::unexpected(::std::move(var).error()); \
    ::std::move(var).value();                                \
  })

#define COROPACT_TRY(expr) COROPACT_TRY_IMPL_(COROPACT_TRY_VAR, expr)
#define COROPACT_CO_TRY(expr) COROPACT_CO_TRY_IMPL_(COROPACT_TRY_VAR, expr)
