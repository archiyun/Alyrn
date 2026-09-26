// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <concepts>
#include <expected>
#include <functional>
#include <initializer_list>
#include <memory>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

#include "alyrn/detail/check.h"

namespace alyrn {

// Default error model used by fallible Alyrn operations.
using Error = std::error_code;

// A fallible result. Alyrn APIs use the default Error type; adapters may
// select a domain-specific error type without introducing a second result
// abstraction.

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

template <typename T, typename E>
class Result;

namespace detail {

template <typename R>
struct IsResult : std::false_type {};

template <typename T, typename E>
struct IsResult<Result<T, E>> : std::true_type {};

}  // namespace detail

template <typename T, typename E = Error>
class Result {
public:
  using ValueType = T;
  using ErrorType = E;

  Result() = default;

  Result(const Result&) = default;
  Result& operator=(const Result&) = default;

  Result(Result&&) = default;
  Result& operator=(Result&&) = default;

  Result(const T& value)
    requires std::copy_constructible<T>
      : result_(value) {}

  Result(T&& value)
    requires std::move_constructible<T>
      : result_(std::move(value)) {}

  template <typename... Args>
    requires std::constructible_from<T, Args...>
  explicit Result(std::in_place_t, Args&&... args)
      : result_(std::in_place, std::forward<Args>(args)...) {}

  template <typename U, typename... Args>
    requires std::constructible_from<T, std::initializer_list<U>&, Args...>
  explicit Result(std::in_place_t, std::initializer_list<U> values, Args&&... args)
      : result_(std::in_place, values, std::forward<Args>(args)...) {}

  template <typename... Args>
    requires std::constructible_from<E, Args...>
  explicit Result(std::unexpect_t, Args&&... args)
      : result_(std::unexpect, std::forward<Args>(args)...) {}

  template <typename G>
  Result(const std::unexpected<G>& error)
    requires std::constructible_from<E, const G&>
      : result_(error) {}

  template <typename G>
  Result(std::unexpected<G>&& error)
    requires std::constructible_from<E, G>
      : result_(std::move(error)) {}

  template <typename G>
  Result& operator=(const std::unexpected<G>& error)
    requires std::assignable_from<E&, const G&>
  {
    result_ = error;
    return *this;
  }

  template <typename G>
  Result& operator=(std::unexpected<G>&& error)
    requires std::assignable_from<E&, G>
  {
    result_ = std::move(error);
    return *this;
  }

  [[nodiscard]]
  bool HasValue() const noexcept {
    return result_.has_value();
  }

  [[nodiscard]]
  explicit operator bool() const noexcept {
    return HasValue();
  }

  [[nodiscard]]
  T& operator*() & noexcept {
    return Value();
  }

  [[nodiscard]]
  const T& operator*() const& noexcept {
    return Value();
  }

  [[nodiscard]]
  T&& operator*() && noexcept {
    return std::move(*this).Value();
  }

  [[nodiscard]]
  const T&& operator*() const&& noexcept {
    return std::move(result_).value();
  }

  [[nodiscard]]
  T* operator->() noexcept {
    return std::addressof(Value());
  }

  [[nodiscard]]
  const T* operator->() const noexcept {
    return std::addressof(Value());
  }

  [[nodiscard]]
  T& Value() & noexcept {
    ALYRN_CHECK(result_.has_value(), "called Result::Value() on an error result");
    return *result_;
  }

  [[nodiscard]]
  const T& Value() const& noexcept {
    ALYRN_CHECK(result_.has_value(), "called Result::Value() on an error result");
    return *result_;
  }

  [[nodiscard]]
  T&& Value() && noexcept {
    ALYRN_CHECK(result_.has_value(), "called Result::Value() on an error result");
    return std::move(*result_);
  }

  [[nodiscard]]
  T& Expect(std::string_view message) & noexcept {
    ALYRN_CHECK(result_.has_value(), message);
    return *result_;
  }

  [[nodiscard]]
  const T& Expect(std::string_view message) const& noexcept {
    ALYRN_CHECK(result_.has_value(), message);
    return *result_;
  }

  [[nodiscard]]
  E& Error() & noexcept {
    ALYRN_CHECK(!result_.has_value(), "called Result::Error() on a value result");
    return result_.error();
  }

  [[nodiscard]]
  const E& Error() const& noexcept {
    ALYRN_CHECK(!result_.has_value(), "called Result::Error() on a value result");
    return result_.error();
  }

  [[nodiscard]]
  E&& Error() && noexcept {
    ALYRN_CHECK(!result_.has_value(), "called Result::Error() on a value result");
    return std::move(result_.error());
  }

  template <typename U>
  [[nodiscard]]
  T ValueOr(U&& fallback) const& {
    return result_.value_or(std::forward<U>(fallback));
  }

  template <typename U>
  [[nodiscard]]
  T ValueOr(U&& fallback) && {
    return std::move(result_).value_or(std::forward<U>(fallback));
  }

  // Monadic composition, mirroring std::expected:
  //   AndThen(f)         f(value) returns Result<U, E>; an error passes through.
  //   Transform(f)       f(value) returns U or void; the result is Result<U, E>.
  //   OrElse(f)          f(error) returns Result<T, G>; a value passes through.
  //   TransformError(f)  f(error) returns G; the result is Result<T, G>.
  // Inside coroutines, ALYRN_CO_TRY / ALYRN_CO_TRY_ASSIGN are usually clearer.
  template <typename F>
  auto AndThen(F&& f) & {
    return AndThenImpl(*this, std::forward<F>(f));
  }

  template <typename F>
  auto AndThen(F&& f) const& {
    return AndThenImpl(*this, std::forward<F>(f));
  }

  template <typename F>
  auto AndThen(F&& f) && {
    return AndThenImpl(std::move(*this), std::forward<F>(f));
  }

  template <typename F>
  auto Transform(F&& f) & {
    return TransformImpl(*this, std::forward<F>(f));
  }

  template <typename F>
  auto Transform(F&& f) const& {
    return TransformImpl(*this, std::forward<F>(f));
  }

  template <typename F>
  auto Transform(F&& f) && {
    return TransformImpl(std::move(*this), std::forward<F>(f));
  }

  template <typename F>
  auto OrElse(F&& f) & {
    return OrElseImpl(*this, std::forward<F>(f));
  }

  template <typename F>
  auto OrElse(F&& f) const& {
    return OrElseImpl(*this, std::forward<F>(f));
  }

  template <typename F>
  auto OrElse(F&& f) && {
    return OrElseImpl(std::move(*this), std::forward<F>(f));
  }

  template <typename F>
  auto TransformError(F&& f) & {
    return TransformErrorImpl(*this, std::forward<F>(f));
  }

  template <typename F>
  auto TransformError(F&& f) const& {
    return TransformErrorImpl(*this, std::forward<F>(f));
  }

  template <typename F>
  auto TransformError(F&& f) && {
    return TransformErrorImpl(std::move(*this), std::forward<F>(f));
  }

private:
  template <typename Self, typename F>
  static auto AndThenImpl(Self&& self, F&& f) {
    using R =
        std::remove_cvref_t<std::invoke_result_t<F, decltype(*std::forward<Self>(self).result_)>>;
    static_assert(detail::IsResult<R>::value, "AndThen requires a callable returning Result");
    static_assert(std::same_as<typename R::ErrorType, E>, "AndThen must keep the error type");
    if (self.result_.has_value()) {
      return R(std::invoke(std::forward<F>(f), *std::forward<Self>(self).result_));
    }
    return R(std::unexpect, std::forward<Self>(self).result_.error());
  }

  template <typename Self, typename F>
  static auto TransformImpl(Self&& self, F&& f) {
    using U =
        std::remove_cv_t<std::invoke_result_t<F, decltype(*std::forward<Self>(self).result_)>>;
    using R = Result<U, E>;
    if (!self.result_.has_value()) {
      return R(std::unexpect, std::forward<Self>(self).result_.error());
    }
    if constexpr (std::is_void_v<U>) {
      std::invoke(std::forward<F>(f), *std::forward<Self>(self).result_);
      return R{};
    } else {
      return R(std::in_place, std::invoke(std::forward<F>(f), *std::forward<Self>(self).result_));
    }
  }

  template <typename Self, typename F>
  static auto OrElseImpl(Self&& self, F&& f) {
    using R = std::remove_cvref_t<
        std::invoke_result_t<F, decltype(std::forward<Self>(self).result_.error())>>;
    static_assert(detail::IsResult<R>::value, "OrElse requires a callable returning Result");
    static_assert(std::same_as<typename R::ValueType, T>, "OrElse must keep the value type");
    if (self.result_.has_value()) {
      return R(std::in_place, *std::forward<Self>(self).result_);
    }
    return R(std::invoke(std::forward<F>(f), std::forward<Self>(self).result_.error()));
  }

  template <typename Self, typename F>
  static auto TransformErrorImpl(Self&& self, F&& f) {
    using G = std::remove_cv_t<
        std::invoke_result_t<F, decltype(std::forward<Self>(self).result_.error())>>;
    using R = Result<T, G>;
    if (self.result_.has_value()) {
      return R(std::in_place, *std::forward<Self>(self).result_);
    }
    return R(std::unexpect,
             std::invoke(std::forward<F>(f), std::forward<Self>(self).result_.error()));
  }

  std::expected<T, E> result_;
};

template <typename E>
class Result<void, E> {
public:
  using ValueType = void;
  using ErrorType = E;

  Result() = default;

  Result(const Result&) = default;
  Result& operator=(const Result&) = default;

  Result(Result&&) = default;
  Result& operator=(Result&&) = default;

  template <typename G>
  Result(const std::unexpected<G>& error)
    requires std::constructible_from<E, const G&>
      : result_(error) {}

  template <typename G>
  Result(std::unexpected<G>&& error)
    requires std::constructible_from<E, G>
      : result_(std::move(error)) {}

  template <typename... Args>
    requires std::constructible_from<E, Args...>
  explicit Result(std::unexpect_t, Args&&... args)
      : result_(std::unexpect, std::forward<Args>(args)...) {}

  [[nodiscard]]
  bool HasValue() const noexcept {
    return result_.has_value();
  }

  [[nodiscard]]
  explicit operator bool() const noexcept {
    return HasValue();
  }

  void Value() const noexcept {
    ALYRN_CHECK(result_.has_value(), "called Result::Value() on an error result");
  }

  void Expect(std::string_view message) const noexcept {
    ALYRN_CHECK(result_.has_value(), message);
  }

  [[nodiscard]]
  E& Error() & noexcept {
    ALYRN_CHECK(!result_.has_value(), "called Result::Error() on a value result");
    return result_.error();
  }

  [[nodiscard]]
  const E& Error() const& noexcept {
    ALYRN_CHECK(!result_.has_value(), "called Result::Error() on a value result");
    return result_.error();
  }

  [[nodiscard]]
  E&& Error() && noexcept {
    ALYRN_CHECK(!result_.has_value(), "called Result::Error() on a value result");
    return std::move(result_.error());
  }

  // Monadic composition, mirroring std::expected:
  //   AndThen(f)         f(value) returns Result<U, E>; an error passes through.
  //   Transform(f)       f(value) returns U or void; the result is Result<U, E>.
  //   OrElse(f)          f(error) returns Result<T, G>; a value passes through.
  //   TransformError(f)  f(error) returns G; the result is Result<T, G>.
  // Inside coroutines, ALYRN_CO_TRY / ALYRN_CO_TRY_ASSIGN are usually clearer.
  template <typename F>
  auto AndThen(F&& f) const {
    using R = std::remove_cvref_t<std::invoke_result_t<F>>;
    static_assert(detail::IsResult<R>::value, "AndThen requires a callable returning Result");
    static_assert(std::same_as<typename R::ErrorType, E>, "AndThen must keep the error type");
    if (result_.has_value()) {
      return R(std::invoke(std::forward<F>(f)));
    }
    return R(std::unexpect, result_.error());
  }

  template <typename F>
  auto Transform(F&& f) const {
    using U = std::remove_cv_t<std::invoke_result_t<F>>;
    using R = Result<U, E>;
    if (!result_.has_value()) {
      return R(std::unexpect, result_.error());
    }
    if constexpr (std::is_void_v<U>) {
      std::invoke(std::forward<F>(f));
      return R{};
    } else {
      return R(std::in_place, std::invoke(std::forward<F>(f)));
    }
  }

  template <typename F>
  auto OrElse(F&& f) const& {
    return OrElseImpl(*this, std::forward<F>(f));
  }

  template <typename F>
  auto OrElse(F&& f) && {
    return OrElseImpl(std::move(*this), std::forward<F>(f));
  }

  template <typename F>
  auto TransformError(F&& f) const& {
    return TransformErrorImpl(*this, std::forward<F>(f));
  }

  template <typename F>
  auto TransformError(F&& f) && {
    return TransformErrorImpl(std::move(*this), std::forward<F>(f));
  }

private:
  template <typename Self, typename F>
  static auto OrElseImpl(Self&& self, F&& f) {
    using R = std::remove_cvref_t<
        std::invoke_result_t<F, decltype(std::forward<Self>(self).result_.error())>>;
    static_assert(detail::IsResult<R>::value, "OrElse requires a callable returning Result");
    static_assert(std::is_void_v<typename R::ValueType>, "OrElse must keep the void value type");
    if (self.result_.has_value()) {
      return R{};
    }
    return R(std::invoke(std::forward<F>(f), std::forward<Self>(self).result_.error()));
  }

  template <typename Self, typename F>
  static auto TransformErrorImpl(Self&& self, F&& f) {
    using G = std::remove_cv_t<
        std::invoke_result_t<F, decltype(std::forward<Self>(self).result_.error())>>;
    using R = Result<void, G>;
    if (self.result_.has_value()) {
      return R{};
    }
    return R(std::unexpect,
             std::invoke(std::forward<F>(f), std::forward<Self>(self).result_.error()));
  }

  std::expected<void, E> result_;
};
}  // namespace alyrn

// Early return for fallible expressions, in the spirit of Rust's `?`. `expr`
// is evaluated once; on failure the enclosing function returns
// std::unexpected(error), so its return type must accept that error.
//
//   ALYRN_TRY(Flush(file));                   // expr is Result<void, E>
//   ALYRN_TRY_ASSIGN(auto size, Parse(text)); // expr is Result<T, E>
//
// The coroutine forms use co_return and accept co_await expressions:
//
//   ALYRN_CO_TRY(co_await stream.Write(bytes));
//   ALYRN_CO_TRY_ASSIGN(auto n, co_await stream.Read(buffer));
//
// The *_ASSIGN forms expand to several statements: use them where a
// declaration may appear (not as the body of an unbraced if), at most once per
// line.
#define ALYRN_DETAIL_TRY_CONCAT_IMPL(a, b) a##b
#define ALYRN_DETAIL_TRY_CONCAT(a, b) ALYRN_DETAIL_TRY_CONCAT_IMPL(a, b)

#define ALYRN_DETAIL_TRY(expr, return_keyword)                              \
  do {                                                                      \
    auto&& alyrn_try_result_ = (expr);                                      \
    if (!alyrn_try_result_.HasValue()) {                                    \
      return_keyword std::unexpected(std::move(alyrn_try_result_).Error()); \
    }                                                                       \
  } while (false)

#define ALYRN_DETAIL_TRY_ASSIGN(lhs, expr, return_keyword, tmp) \
  auto&& tmp = (expr);                                          \
  if (!tmp.HasValue()) {                                        \
    return_keyword std::unexpected(std::move(tmp).Error());     \
  }                                                             \
  lhs = std::move(tmp).Value()

#define ALYRN_TRY(expr) ALYRN_DETAIL_TRY(expr, return)
#define ALYRN_CO_TRY(expr) ALYRN_DETAIL_TRY(expr, co_return)
#define ALYRN_TRY_ASSIGN(lhs, expr) \
  ALYRN_DETAIL_TRY_ASSIGN(lhs, expr, return, ALYRN_DETAIL_TRY_CONCAT(alyrn_try_result_, __LINE__))
#define ALYRN_CO_TRY_ASSIGN(lhs, expr)          \
  ALYRN_DETAIL_TRY_ASSIGN(lhs, expr, co_return, \
                          ALYRN_DETAIL_TRY_CONCAT(alyrn_try_result_, __LINE__))
