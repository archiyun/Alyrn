// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once
#include <liburing.h>

#include <cerrno>
#include <stdexcept>

#include "runtime/base/noncopyable.h"

namespace runtime::uring {

// 目前仅在 demo 阶段， 可忽略
// io_uring 实例的薄 RAII 封装。只管准备/提交 SQE；
// 收割 CQE 不在这里做——那是 UringLoop 的事（它知道怎么把 CQE 派发给 user_data 里的 Completion）。
class Ring : public runtime::base::NonCopyable {
public:
  explicit Ring(unsigned entries = 256) {
    const int rc = io_uring_queue_init(entries, &ring_, /*flags=*/0);
    if (rc < 0) throw std::runtime_error("io_uring_queue_init failed");
  }
  ~Ring() { io_uring_queue_exit(&ring_); }

  // SQ 满时先 flush 一次再取；仍可能返回 nullptr。
  io_uring_sqe* get_sqe() {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
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

}  // namespace runtime::uring
