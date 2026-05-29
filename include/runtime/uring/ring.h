// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once
#include <liburing.h>

#include <stdexcept>

#include "runtime/base/noncopyable.h"

namespace runtime::uring {

// 目前仅在 demo 阶段， 可忽略
// io_uring 的一层封装。
class Ring : public runtime::base::NonCopyable {};

}  // namespace runtime::uring
