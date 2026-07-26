// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

#include "coropact/base/error.h"
#include "coropact/coro/work.h"

namespace coropact::luring {

enum class LUringOpKind : std::uint8_t {
  kNop,
  kRead,
  kWrite,
  kAccept,
  kConnect,
  kClose,
  kCancelAll,
  kTimeout,
  kMsgRing,
  kWake,
  kCount,
};

// A CQE result is always an integer. Kernel errors are represented by a
// negative cqe_res and converted to base::Error by the awaiter, so storing a
// full std::expected<int, Error> here needlessly adds the error union and its
// alignment padding to every operation.
class LUringCqeResult {
public:
  constexpr LUringCqeResult() noexcept = default;

  LUringCqeResult& operator=(int value) noexcept {
    value_ = value;
    engaged_ = true;
    return *this;
  }

  [[nodiscard]]
  bool has_value() const noexcept {
    return engaged_;
  }

  [[nodiscard]]
  int& operator*() noexcept {
    assert(engaged_);
    return value_;
  }

  [[nodiscard]]
  const int& operator*() const noexcept {
    assert(engaged_);
    return value_;
  }

  [[nodiscard]]
  base::Error error() const noexcept {
    assert(!engaged_);
    return base::make_errno(EIO);
  }

private:
  int value_{0};
  bool engaged_{false};
};

static_assert(sizeof(LUringCqeResult) == 8);

class LUringOp {
public:
  using CompleteHook = void (*)(LUringOp*) noexcept;

  coro::ResumeWork resume_work;
  LUringCqeResult result;
  void* owner{nullptr};
  CompleteHook on_complete{nullptr};
  LUringOpKind kind{};
  bool completed{false};

  [[nodiscard]]
  bool Complete(int cqe_res) noexcept {
    if (completed) {
      return false;
    }

    result = cqe_res;
    completed = true;
    if (on_complete != nullptr) {
      on_complete(this);
    }
    return true;
  }
};

static_assert(sizeof(LUringOp) == 48);

}  // namespace coropact::luring
