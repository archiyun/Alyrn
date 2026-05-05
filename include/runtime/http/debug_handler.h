#pragma once

#include "runtime/task/task_record.h"

#include <string>
#include <vector>

namespace runtime::http {

// Serializes a vector of TaskRecords to a JSON object:
// { "capacity": N, "count": M, "tasks": [...] }
std::string MakeDebugTasksJson(const std::vector<runtime::task::TaskRecord>& records,
                               std::size_t capacity);

}  // namespace runtime::http
