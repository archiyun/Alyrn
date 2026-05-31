// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <functional>

namespace runtime::uring {

// 每个 io_uring 操作的"续延", 指向它的指针存进 SQE 的 user_data
// UringLoop 收到对应的 CQE
struct Completion {
  std::function<void(int res, unsigned flags)> OnComplete;
};

}  // namespace runtime::uring
