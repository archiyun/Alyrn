// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/task/detail/task.h"

namespace vexo::task {

Task::Task(uint64_t id, Func func) : id(id), func(std::move(func)) {}

}  // namespace vexo::task
