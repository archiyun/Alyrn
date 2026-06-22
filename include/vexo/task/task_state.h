// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
namespace vexo::task {

// desc taskstate
enum class TaskState : uint8_t {
  kPending,    // in WorkQueue, waiting for a worker
  kRunning,    // executing on a worker thread
  kCompleted,  // func() returned normally
  kFailed,     // func() threw an exception
  kCancelled,  // cancelled before or during execution
};

}  // namespace vexo::task
