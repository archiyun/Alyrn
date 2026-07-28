// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>

#include "coropact/base/error.h"

namespace coropact::reactor::detail {

// Reactor read/write attempts only need to retain a non-negative byte count or
// a positive errno while the coroutine is suspended. Store both in one signed
// word: non-negative values are byte counts, negative values are -errno, and
// INT64_MIN marks the not-yet-completed state.
class ReactorIoResultState {
public:
  static constexpr std::int64_t kPending = std::numeric_limits<std::int64_t>::min();

  [[nodiscard]]
  bool HasResult() const noexcept {
    return encoded_ != kPending;
  }

  void SetSuccess(std::size_t bytes) noexcept {
    assert(bytes <= static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()));
    encoded_ = static_cast<std::int64_t>(bytes);
  }

  void SetError(base::Error error) noexcept {
    const int value = error.value();
    assert(value > 0);
    encoded_ = -static_cast<std::int64_t>(value);
  }

  void SetResult(const base::Result<std::size_t>& result) noexcept {
    if (result.has_value()) {
      SetSuccess(*result);
    } else {
      SetError(result.error());
    }
  }

  [[nodiscard]]
  base::Result<std::size_t> Take() const noexcept {
    assert(HasResult());
    if (encoded_ >= 0) {
      return static_cast<std::size_t>(encoded_);
    }
    return std::unexpected(base::MakeErrno(static_cast<int>(-encoded_)));
  }

private:
  std::int64_t encoded_{kPending};
};

static_assert(sizeof(ReactorIoResultState) == sizeof(std::int64_t));

// Result storage for non-trivial Reactor values such as ReactorStream. The
// expected/optional pair carries two readiness flags and an extra padding byte;
// this state keeps one explicit tag and constructs the stream only on success.
template <typename T>
class ReactorValueResultState {
public:
  ReactorValueResultState() noexcept = default;

  ~ReactorValueResultState() { Reset(); }

  ReactorValueResultState(const ReactorValueResultState&) = delete;
  ReactorValueResultState& operator=(const ReactorValueResultState&) = delete;

  [[nodiscard]]
  bool HasResult() const noexcept {
    return state_ != State::kPending;
  }

  void SetError(base::Error error) noexcept {
    Reset();
    ::new (static_cast<void*>(&storage_.error)) base::Error(error);
    state_ = State::kError;
  }

  void SetResult(base::Result<T>&& result) noexcept(
      std::is_nothrow_move_constructible_v<T>) {
    Reset();
    if (result.has_value()) {
      ::new (static_cast<void*>(&storage_.value)) T(std::move(*result));
      state_ = State::kValue;
    } else {
      SetError(result.error());
    }
  }

  [[nodiscard]]
  base::Result<T> Take() noexcept(std::is_nothrow_move_constructible_v<T>) {
    assert(HasResult());
    if (state_ == State::kError) {
      const base::Error error = storage_.error;
      state_ = State::kPending;
      return std::unexpected(error);
    }

    base::Result<T> result(std::in_place, std::move(storage_.value));
    storage_.value.~T();
    state_ = State::kPending;
    return result;
  }

private:
  enum class State : std::uint8_t {
    kPending,
    kValue,
    kError,
  };

  union Storage {
    Storage() {}
    ~Storage() {}

    T value;
    base::Error error;
  } storage_;
  State state_{State::kPending};

  void Reset() noexcept {
    if (state_ == State::kValue) {
      storage_.value.~T();
    }
    state_ = State::kPending;
  }
};

}  // namespace coropact::reactor::detail
