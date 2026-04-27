#pragma once

#include <cstdint>
#include <string>

namespace runtime::task {

enum class TaskPriority : int {
  kLow = 0,
  kNormal = 10,
  kHigh = 20,
};

struct TaskOptions {
  std::string name;
  TaskPriority priority{TaskPriority::kNormal};
  uint32_t timeout_ms{0}; // 0 = no time out (Phase 4)
};

}; // namespace runtime::task
