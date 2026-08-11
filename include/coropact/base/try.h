// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <expected>
#include <utility>

// Standard-C++ error propagation helpers for functions returning an expected
// with the same error type as expr.
//
//   COROPACT_TRY_VALUE(value, ReadValue());
//   COROPACT_TRY(RecordSideEffect());
//   COROPACT_CO_TRY(value, co_await ReadValueAsync());

#define COROPACT_TRY(expr)                                  \
  do {                                                       \
    auto coropact_try_result_ = (expr);                      \
    if (!coropact_try_result_.has_value()) [[unlikely]] {    \
      return ::std::unexpected(                              \
          ::std::move(coropact_try_result_).error());         \
    }                                                         \
  } while (false)

// Declares `name` from an expected expression after propagating an error.
// `name` must be a plain identifier; the helper derives a local storage name
// from it. Like a variable declaration, this macro must be a standalone
// statement in its enclosing scope.
#define COROPACT_TRY_VALUE(name, expr)                             \
  auto name##_coropact_try_result_ = (expr);                        \
  if (!name##_coropact_try_result_.has_value()) [[unlikely]] {      \
    return ::std::unexpected(                                        \
        ::std::move(name##_coropact_try_result_).error());           \
  }                                                                   \
  auto name = ::std::move(name##_coropact_try_result_).value()

#define COROPACT_CO_TRY_IMPL_(name, var, expr)              \
  auto var = (expr);                                         \
  if (!var.has_value()) [[unlikely]] {                       \
    co_return ::std::unexpected(::std::move(var).error());   \
  }                                                            \
  auto name = ::std::move(var).value()

#define COROPACT_CO_TRY(name, expr) \
  COROPACT_CO_TRY_IMPL_(name, name##_coropact_co_try_result_, expr)
