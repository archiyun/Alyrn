// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <cstddef>
#include <cstdint>

#include "vexo/base/error.h"
#include "vexo/coro/work.h"

namespace vexo::luring {

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
  vexo::coro::ResumeWork resume_work;
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

}  // namespace vexo::luring
