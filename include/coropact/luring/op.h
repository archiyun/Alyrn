// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
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
  kTimeout,
  kMsgRing,
  kWake,
  kCount,
};

class LUringOp {
public:
  using CompleteHook = void (*)(LUringOp*) noexcept;

  LUringOpKind kind{};
  std::coroutine_handle<> continuation_;
  coropact::coro::ResumeWork resume_work;
  base::Result<int> result;
  bool completed{false};

  void* owner{nullptr};
  CompleteHook on_complete{nullptr};

  void Complete(int cqe_res) noexcept {
    result = cqe_res;
    completed = true;
    if (on_complete != nullptr) {
      on_complete(this);
    }
  }
};

}  // namespace coropact::luring
