// SPDX-License-Identifier: MIT
#pragma once

#include <liburing.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>

#include "alyrn/result.h"
#include "alyrn/uring/options.h"
#include "alyrn/detail/macros.h"

namespace alyrn::uring::detail {

// Thin RAII wrapper around a single io_uring instance.
//
// This class only manages the low-level ring lifecycle and exposs the minimal
// operations requires by Loop:
//   - initialize and destroy the ring
//   - acquire SQEs
//   - submit prepared SQEs
//   - reap completed CQEs
//
// Scheduling coroutine resumption and operation lifetime are handled by
// Loop rather than this class.
class Ring {
public:
  ALYRN_DELETE_COPY(Ring);

  Ring() = default;
  ~Ring() noexcept;

  Ring(Ring&& other) noexcept;
  Ring& operator=(Ring&& other) noexcept;

  static Result<Ring> Create(const Options& options) noexcept;

  io_uring_sqe* GetSqe() noexcept;
  Result<std::size_t> Submit() noexcept;
  Result<void> GetEvents() noexcept;

  int Fd() const noexcept { return initialized_ ? ring_.ring_fd : -1; }

  template <class F>
  Result<std::size_t> Reap(F&& on_cqe, std::size_t max_count = 0) noexcept {
    io_uring_cqe* cqe = nullptr;
    int result = io_uring_peek_cqe(&ring_, &cqe);
    if (result == -EAGAIN) {
      return std::size_t{0};
    }
    if (result < 0) {
      return std::unexpected(NegErrno(result));
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

  io_uring* Native() noexcept { return &ring_; }

private:
  explicit Ring(io_uring ring) noexcept : ring_(ring), initialized_(true) {}

  io_uring ring_;
  bool initialized_{false};
};

}  // namespace alyrn::uring::detail
