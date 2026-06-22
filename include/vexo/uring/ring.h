// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <liburing.h>

#include <optional>

#include "vexo/base/noncopyable.h"

namespace vexo::uring {

// RAII owner of a liburing io_uring instance (the SQ/CQ ring pair).
//
// Setup can fail
class Ring : public vexo::base::NonCopyable {
public:
  [[nodiscard]] std::optional<Ring> Create(unsigned entries);
  ~Ring(); // io_uring_queue_exit
  Ring(Ring&& other) noexcept;
  Ring& operator=(Ring&& other) noexcept;

  [[nodiscard]] io_uring_sqe* get_sqe() { return io_uring_get_sqe(&ring_); }

  int Submit() { return io_uring_submit(&ring_); }

  int SubmitAndWait(unsigned wait_nr) {
    return io_uring_submit_and_wait(&ring_, wait_nr);
  }

  [[nodiscard]] io_uring* Raw() { return &ring_; }

private:
  explicit Ring(const io_uring& ring) : ring_(ring), valid_(true) {}
  io_uring ring_;
  bool valid_{false};
};

}  // namespace vexo::uring
