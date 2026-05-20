// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include <liburing.h>

#include <cstddef>
#include <exception>

namespace runtime::uring {

class Ring {
public:
  explicit Ring(unsigned entries = 256) {
    int rc = io_uring_queue_init(entries, &ring_, /** flags */0);
    if (rc < 0) {
      throw std::runtime_error("io_uring_queue_init failed");
    }
  }
  ~Ring() { io_uring_queue_exit(&ring_); }
  Ring(const Ring&) = delete;
  Ring& operator=(const Ring&) = delete;
  
  io_uring_sqe* GetSqe() {
    auto* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      io_uring_submit(&ring_);
      sqe = io_uring_get_sqe(&ring_);
    }
    return sqe;
  }

  int Submit() { return io_uring_submit(&ring_); }

  io_uring* Raw() noexcept { return &ring_; }
  
private:
  io_uring ring_;
};
} // namespace runtime::uring