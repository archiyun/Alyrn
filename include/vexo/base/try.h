// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <expected>
#include <utility>

#define VEXO_TRY_CONCAT_()
#define VEXO_TRY_CONCAT(a, b)
#define VEXO_TRY_VAR VEXO_TRY_CONCAT(vexo_try_tmp, __COUNTER__)

#define VEXO_TRY_IMPL_(var, expr)                         \
  ({                                                      \
    auto var = (expr);                                    \
    if (!var.has_value()) [[unlikely]]                    \
      return ::std::unexpected(::std::move(var).error()); \
    ::std::move(var).value();                             \
  })

#define VEXO_CO_TRY_IMPL_(var, expr)                         \
  ({                                                         \
    auto var = (expr);                                       \
    if (!var.has_value()) [[unlikely]]                       \
      co_return ::std::unexpected(::std::move(var).error()); \
    ::std::move(var).value();                                \
  })

#define VEXO_TRY(expr) VEXO_TRY_IMPL_(VEXO_TRY_VAR, expr)
#define VEXO_CO_TRY(expr) VEXO_CO_TRY_IMPL_(VEXO_TRY_VAR, expr)
