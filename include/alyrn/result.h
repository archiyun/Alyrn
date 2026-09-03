// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <concepts>
#include <expected>
#include <initializer_list>
#include <memory>
#include <string_view>
#include <system_error>
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

template <typename T, typename E = Error>
class Result {
public:
  using ValueType = T;
  using ErrorType = E;

  Result() = default;

  Result(const Result&) = default;
  Result& operator=(const Result&) = default;

  Result(Result&&) noexcept = default;
  Result& operator=(Result&&) noexcept = default;

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
  bool has_value() const noexcept {
    return HasValue();
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
  T& value() & noexcept {
    return Value();
  }

  [[nodiscard]]
  const T& value() const& noexcept {
    return Value();
  }

  [[nodiscard]]
  T&& value() && noexcept {
    return std::move(*this).Value();
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

  [[nodiscard]]
  E& error() & noexcept {
    return Error();
  }

  [[nodiscard]]
  const E& error() const& noexcept {
    return Error();
  }

  [[nodiscard]]
  E&& error() && noexcept {
    return std::move(*this).Error();
  }

  template <typename U>
  [[nodiscard]]
  T value_or(U&& fallback) const& {
    return result_.value_or(std::forward<U>(fallback));
  }

  template <typename U>
  [[nodiscard]]
  T value_or(U&& fallback) && {
    return std::move(result_).value_or(std::forward<U>(fallback));
  }

private:
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

  Result(Result&&) noexcept = default;
  Result& operator=(Result&&) noexcept = default;

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
  bool has_value() const noexcept {
    return HasValue();
  }

  [[nodiscard]]
  explicit operator bool() const noexcept {
    return HasValue();
  }

  void Value() const noexcept {
    ALYRN_CHECK(result_.has_value(), "called Result::Value() on an error result");
  }

  void value() const noexcept { Value(); }

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

  [[nodiscard]]
  E& error() & noexcept {
    return Error();
  }

  [[nodiscard]]
  const E& error() const& noexcept {
    return Error();
  }

  [[nodiscard]]
  E&& error() && noexcept {
    return std::move(*this).Error();
  }

private:
  std::expected<void, E> result_;
};
}  // namespace alyrn
