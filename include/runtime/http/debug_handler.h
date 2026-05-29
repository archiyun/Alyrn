// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <string>
#include <vector>

#include "runtime/task/task_record.h"

namespace runtime::http {

// Serializes a vector of TaskRecords to a JSON object:
// { "capacity": N, "count": M, "tasks": [...] }
std::string MakeDebugTasksJson(const std::vector<runtime::task::TaskRecord>& records,
                               std::size_t capacity);

}  // namespace runtime::http
