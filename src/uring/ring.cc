// CopyRight (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/uring/ring.h"
#include "vexo/log/logger.h"

namespace vexo::uring {

std::optional<Ring> Create(unsigned entries) {
  io_uring ring{};
  const int rc = io_uring_queue_init(entries, &ring, /*flags=*/0);
  if (rc < 0) LOG_WARN() << "ring.cc io_uring_init failed\n";
  return Ring(ring);
}
}  // namespace vexo::uring
