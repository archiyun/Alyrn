#pragma once

#include <cstddef>
#include <cstdint>

namespace vexo::luring {

// LUringOptions configures the ownership, queue sizing, polling mode,
// and submission batching policy of one io_uring-backed event loop.
struct LUringOptions {
  std::uint32_t entries{4096};
  std::uint32_t cq_entries{0};

  bool setup_sqpoll{false};
  std::uint32_t sqpoll_idle_ms{1000};

  bool setup_iopoll{false};  // don't set it true.
  bool setup_submit_all{true};
  bool setup_single_issuer{true};

  std::size_t submit_batch{32};
};

}  // namespace vexo::luring
