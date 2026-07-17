// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <liburing.h>

#include <cerrno>
#include <cstddef>

#include "vexo/base/error.h"
#include "vexo/luring/options.h"
#include "vexo/utils/macros.h"

namespace vexo::luring {

// 只封装 io_uring 初始化, get_sqe, submit, reap_cqe.
class LUringRing {
public:
  VEXO_DELETE_COPY(LUringRing);
  LUringRing() = default;
  ~LUringRing();

  LUringRing(LUringRing&& other) noexcept;
  LUringRing& operator=(LUringRing&& other) noexcept;

  [[nodiscard]] static base::Result<LUringRing> Create(const LUringOptions& options) noexcept;

  [[nodiscard]] io_uring_sqe* GetSqe() noexcept;
  [[nodiscard]] base::Result<std::size_t> Submit() noexcept;

  template <class F>
  [[nodiscard]] base::Result<std::size_t> Reap(F&& on_cqe) noexcept {
    io_uring_cqe* cqe = nullptr;
    int r = io_uring_peek_cqe(&ring_, &cqe);
    if (r == -EAGAIN) return std::size_t{0};
    if (r < 0) return std::unexpected(base::make_neg_errno(r));

    unsigned head = 0;
    std::size_t n = 0;
    io_uring_for_each_cqe(&ring_, head, cqe) {
      on_cqe(cqe);
      ++n;
    }
    io_uring_cq_advance(&ring_, static_cast<unsigned>(n));
    return n;
  }

  io_uring* native() noexcept { return &ring_; }

private:
  explicit LUringRing(io_uring ring) noexcept : ring_(ring), initialized_(true) {}

  io_uring ring_;
  bool initialized_{false};
};

}  // namespace vexo::luring
