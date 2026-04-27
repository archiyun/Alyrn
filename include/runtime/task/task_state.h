#pragma once

#include <cstdint>
namespace runtime::task {

// 任务生命周期状态机
enum class TaskState : uint8_t {
  kPending,
  kRunning,
  kCompleted,
  kFailed,
  kCancelled,
  kTimeout,
};

}  // namespace runtime::task