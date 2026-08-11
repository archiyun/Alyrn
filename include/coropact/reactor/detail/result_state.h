// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <system_error>
#include <type_traits>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor::detail {

// Reactor read/write attempts only need to retain a non-negative byte count or
// a positive system errno while the coroutine is suspended. Store both in one
// signed word: non-negative values are byte counts, negative values are
// -errno, and INT64_MIN marks the not-yet-completed state.
class ReactorIoResultState {
public:
  static constexpr std::int64_t kPending = std::numeric_limits<std::int64_t>::min();

  [[nodiscard]]
  bool HasResult() const noexcept {
    return encoded_ != kPending;
  }

  void SetSuccess(std::size_t bytes) noexcept {
    COROPACT_CHECK(!HasResult(), "ReactorIoResultState result was set twice");
    COROPACT_CHECK(bytes <= static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()),
                   "ReactorIoResultState byte count cannot be encoded");
    encoded_ = static_cast<std::int64_t>(bytes);
  }

  void SetError(base::Error error) noexcept {
    COROPACT_CHECK(!HasResult(), "ReactorIoResultState result was set twice");
    COROPACT_CHECK(error.category() == std::system_category(),
                   "ReactorIoResultState only encodes system errors");
    const int value = error.value();
    COROPACT_CHECK(value > 0, "ReactorIoResultState cannot encode errno zero");
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
  base::Result<std::size_t> Take() noexcept {
    COROPACT_CHECK(HasResult(), "ReactorIoResultState result was taken before completion");
    const std::int64_t encoded = std::exchange(encoded_, kPending);
    if (encoded >= 0) {
      return static_cast<std::size_t>(encoded);
    }
    return std::unexpected(base::MakeErrno(static_cast<int>(-encoded)));
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
  COROPACT_DELETE_COPY(ReactorValueResultState);

  ReactorValueResultState() noexcept = default;
  ~ReactorValueResultState() { Reset(); }

  [[nodiscard]]
  bool HasResult() const noexcept {
    return state_ != State::kPending;
  }

  void SetError(base::Error error) noexcept {
    COROPACT_CHECK(state_ == State::kPending,
                   "ReactorValueResultState result was set twice");
    std::construct_at(&storage_.error, std::move(error));
    state_ = State::kError;
  }

  void SetResult(base::Result<T>&& result) noexcept(std::is_nothrow_move_constructible_v<T>) {
    COROPACT_CHECK(state_ == State::kPending,
                   "ReactorValueResultState result was set twice");
    if (result.has_value()) {
      std::construct_at(&storage_.value, std::move(*result));
      state_ = State::kValue;
    } else {
      std::construct_at(&storage_.error, std::move(result.error()));
      state_ = State::kError;
    }
  }

  [[nodiscard]]
  base::Result<T> Take() noexcept(std::is_nothrow_move_constructible_v<T>) {
    COROPACT_CHECK(HasResult(), "ReactorValueResultState result was taken before completion");
    if (state_ == State::kError) {
      base::Error error = std::move(storage_.error);
      std::destroy_at(&storage_.error);
      state_ = State::kPending;
      return std::unexpected(std::move(error));
    }

    base::Result<T> result(std::in_place, std::move(storage_.value));
    std::destroy_at(&storage_.value);
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
    switch (state_) {
      case State::kValue:
        std::destroy_at(&storage_.value);
        break;
      case State::kError:
        std::destroy_at(&storage_.error);
        break;
      case State::kPending:
        break;
    }
    state_ = State::kPending;
  }
};

}  // namespace coropact::reactor::detail
