// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdint>

namespace coropact::luring {

// Options configures resources whose size or kernel execution model is
// observable to the application. Submission and scheduling policy are fixed
// implementation details of Loop.
struct Options {
  std::uint32_t entries{4096};

  // SQPOLL creates a kernel submission thread per ring and is opt-in.
  bool setup_sqpoll{false};
  std::uint32_t sqpoll_idle_ms{1000};

  // Loop-wide provided-buffer ring used by RecvSources. This is an aggregate
  // per-worker upper bound, shared by all sources on the loop. Slots are
  // published lazily as sources are created. Set it to zero to disable
  // RecvSource creation.
  std::size_t shared_buffer_capacity{64};
  std::size_t shared_buffer_size{16 * 1024};
};

}  // namespace coropact::luring
