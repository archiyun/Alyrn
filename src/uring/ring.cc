// CopyRight (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/uring/ring.h"
#include "runtime/log/logger.h"

namespace runtime::uring {

std::optional<Ring> Create(unsigned entries) {
  io_uring ring{};
  const int rc = io_uring_queue_init(entries, &ring, /*flags=*/0);
  if (rc < 0) LOG_WARN() << "ring.cc io_uring_init failed\n";
  return Ring(ring);
}
}  // namespace runtime::uring
