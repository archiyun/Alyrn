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

  // Maximum number of ready coroutine works resumed before polling CQEs
  // again. This bounds scheduler monopolization under a completion burst.
  // Zero disables the budget and drains the ready queue completely.
  std::size_t max_ready_work_per_turn{256};
};

}  // namespace vexo::luring
