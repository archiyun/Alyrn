// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <liburing.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>

#include "vexo/base/error.h"
#include "vexo/luring/options.h"
#include "vexo/utils/macros.h"

namespace vexo::luring {

// Thin RAII wrapper around a single io_uring instance.
//
// This class only manages the low-level ring lifecycle and exposs the minimal
// operations requires by LUringLoop:
//   - initialize and destroy the ring
//   - acquire SQEs
//   - submit prepared SQEs
//   - reap completed CQEs
//
// Scheduling coroutine resumption and operation lifetime are handled by
// LUringLoop rather than this class.
class LUringRing {
public:
  VEXO_DELETE_COPY(LUringRing);

  LUringRing() = default;
  ~LUringRing() noexcept;

  LUringRing(LUringRing&& other) noexcept;
  LUringRing& operator=(LUringRing&& other) noexcept;

  [[nodiscard]] static base::Result<LUringRing> Create(const LUringOptions& options) noexcept;

  [[nodiscard]] io_uring_sqe* GetSqe() noexcept;
  [[nodiscard]] base::Result<std::size_t> Submit() noexcept;

  void PrepMsgRing(io_uring_sqe* sqe, int target_ring_fd, std::uint32_t type,
                   std::uint64_t data) noexcept;
  [[nodiscard]] int fd() const noexcept { return initialized_ ? ring_.ring_fd : -1; }

  template <class F>
  [[nodiscard]] base::Result<std::size_t> Reap(F&& on_cqe, std::size_t max_count = 0) noexcept {
    io_uring_cqe* cqe = nullptr;
    int result = io_uring_peek_cqe(&ring_, &cqe);
    if (result == -EAGAIN) {
      return std::size_t{0};
    }
    if (result < 0) {
      return std::unexpected(base::make_neg_errno(result));
    }

    unsigned head = 0;
    std::size_t count = 0;
    io_uring_for_each_cqe(&ring_, head, cqe) {
      if (max_count != 0 && count >= max_count) {
        break;
      }
      on_cqe(cqe);
      ++count;
    }
    io_uring_cq_advance(&ring_, static_cast<unsigned>(count));
    return count;
  }

  io_uring* native() noexcept { return &ring_; }

private:
  explicit LUringRing(io_uring ring) noexcept : ring_(ring), initialized_(true) {}

  io_uring ring_;
  bool initialized_{false};
};

}  // namespace vexo::luring
