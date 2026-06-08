// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/task/detail/task.h"

namespace runtime::task {

Task::Task(uint64_t id, Func func) : id(id), func(std::move(func)) {}

}  // namespace runtime::task
