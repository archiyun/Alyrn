// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdint>

namespace alyrn::uring {

// Controls when the Linux io_uring kernel runs deferred task work. This is a
// ring execution policy, not a coroutine scheduler policy: it does not change
// coroutine placement, readiness ordering, or continuation resumption.
enum class TaskRunMode : uint8_t {
  // Let the kernel use its normal task-work behavior.
  kDefault,
  // Ask io_uring to prefer running task work cooperatively from submitters.
  kCooperative,
  // Defer task work until the owning thread enters io_uring explicitly.
  kDeferred,
};

// Options configures resources whose size or kernel execution model is
// observable to the application. Submission and scheduling policy are fixed
// implementation details of Loop.
struct Options {
  std::uint32_t entries{4096};

  // SQPOLL creates a kernel submission thread per ring and is opt-in.
  bool setup_sqpoll{false};
  std::uint32_t sqpoll_idle_ms{1000};

  // Selects the io_uring kernel task-work mode for this Loop's ring. It does
  // not configure the coroutine task scheduler; coroutine scheduling remains
  // an implementation detail of Loop. kDeferred requires the loop to enter
  // the kernel regularly, including while draining during shutdown.
  TaskRunMode task_run_mode{TaskRunMode::kDefault};

  // Loop-wide provided-buffer ring used by RecvSources and Recv(). This is an
  // aggregate per-worker upper bound, shared by all sources on the loop. Slots
  // are published lazily as sources are created. Set it to zero to disable
  // RecvSource creation and Recv().
  std::size_t shared_buffer_capacity{64};
  std::size_t shared_buffer_size{16 * 1024};
};

}  // namespace alyrn::uring
