// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <expected>
#include <utility>

// These macros use the GNU statement-expression extension so a failed
// expected can return from the enclosing function while the successful value
// remains usable as an expression. The project targets GCC/Clang on Linux.
//
//   const auto value = COROPACT_TRY(ReadValue());
//   COROPACT_CO_TRY(value, co_await ReadValueAsync());
//
// COROPACT_CO_TRY intentionally takes a destination name. Keeping the
// coroutine form as a statement avoids a GCC ICE when co_return appears in a
// statement expression.
#define COROPACT_DETAIL_TRY_CONCAT_IMPL_(a, b) a##b
#define COROPACT_DETAIL_TRY_CONCAT_(a, b) COROPACT_DETAIL_TRY_CONCAT_IMPL_(a, b)
#define COROPACT_DETAIL_TRY_VAR \
  COROPACT_DETAIL_TRY_CONCAT_(coropact_try_tmp_, __COUNTER__)

#define COROPACT_TRY_IMPL_(var, expr)                              \
  ({                                                               \
    auto var = (expr);                                             \
    if (!var.has_value()) [[unlikely]]                             \
      return ::std::unexpected(::std::move(var).error());          \
    ::std::move(var).value();                                      \
  })

#define COROPACT_CO_TRY_IMPL_(name, var, expr)                 \
  auto var = (expr);                                          \
  if (!var.has_value()) [[unlikely]]                          \
    co_return ::std::unexpected(::std::move(var).error());    \
  auto name = ::std::move(var).value()

#define COROPACT_TRY(expr) \
  COROPACT_TRY_IMPL_(COROPACT_DETAIL_TRY_VAR, expr)
#define COROPACT_CO_TRY(name, expr) \
  COROPACT_CO_TRY_IMPL_(name, COROPACT_DETAIL_TRY_VAR, expr)
