// SPDX-License-Identifier: MIT
#pragma once

// Shared adapter-contract header for epoll and uring. Include this
// file directly; there is no alyrn/backend.h. Applications use alyrn/io.h.

#include <cstdint>
#include <expected>
#include <memory>
#include <type_traits>
#include <utility>

#include "alyrn/detail/check.h"
#include "alyrn/result.h"
#include "alyrn/detail/macros.h"

namespace alyrn::backend {

// Storage for a non-trivial Result<T>. Backend adapters use it to retain
// a result across suspension; a separate operation lifecycle authorizes when
// that result becomes observable and when its continuation may run.
template <typename T>
class ValueResultState {
public:
  ALYRN_DELETE_COPY(ValueResultState);

  ValueResultState() noexcept = default;
  ~ValueResultState() { Reset(); }

  bool HasResult() const noexcept {
    return state_ != State::kPending;
  }

  void SetError(Error error) noexcept {
    ALYRN_CHECK(state_ == State::kPending, "ValueResultState result was set twice");
    std::construct_at(&storage_.error, std::move(error));
    state_ = State::kError;
  }

  void SetResult(Result<T>&& result) noexcept(std::is_nothrow_move_constructible_v<T>) {
    ALYRN_CHECK(state_ == State::kPending, "ValueResultState result was set twice");
    if (result.HasValue()) {
      std::construct_at(&storage_.value, std::move(*result));
      state_ = State::kValue;
    } else {
      std::construct_at(&storage_.error, std::move(result.Error()));
      state_ = State::kError;
    }
  }

  Result<T> Take() noexcept(std::is_nothrow_move_constructible_v<T>) {
    ALYRN_CHECK(HasResult(), "ValueResultState result was taken before completion");
    if (state_ == State::kError) {
      Error error = std::move(storage_.error);
      std::destroy_at(&storage_.error);
      state_ = State::kPending;
      return std::unexpected(std::move(error));
    }

    Result<T> result(std::in_place, std::move(storage_.value));
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
    Error error;
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

}  // namespace alyrn::backend
