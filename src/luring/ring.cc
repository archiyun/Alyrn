// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/luring/ring.h"

#include <liburing.h>
#include <liburing/io_uring.h>

#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/luring/options.h"

namespace vexo::luring {

namespace {

// Convert the high-level LUringOptions into native liburing parameters.
[[nodiscard]] io_uring_params MakeParams(const LUringOptions& options) noexcept {
  io_uring_params params{};
  params.flags |= IORING_SETUP_CLAMP;

  if (options.setup_submit_all) {
    params.flags |= IORING_SETUP_SUBMIT_ALL;
  }

  if (options.setup_single_issuer) {
    params.flags |= IORING_SETUP_SINGLE_ISSUER;
  }

  if (options.cq_entries != 0) {
    params.flags |= IORING_SETUP_CQSIZE;
    params.cq_entries = options.cq_entries;
  }

  if (options.setup_sqpoll) {
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = options.sqpoll_idle_ms;
  }

  if (options.setup_iopoll) {
    params.flags |= IORING_SETUP_IOPOLL;
  }
  return params;
}

}  // namespace

LUringRing::~LUringRing() {
  if (initialized_) {
    io_uring_queue_exit(&ring_);
  }
}

LUringRing::LUringRing(LUringRing&& other) noexcept
    : ring_(other.ring_), initialized_(std::exchange(other.initialized_, false)) {
  std::memset(&other.ring_, 0, sizeof(other.ring_));
}

LUringRing& LUringRing::operator=(LUringRing&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  if (initialized_) {
    io_uring_queue_exit(&ring_);
  }

  ring_ = other.ring_;
  initialized_ = std::exchange(other.initialized_, false);
  std::memset(&other.ring_, 0, sizeof(other.ring_));
  return *this;
}

base::Result<LUringRing> LUringRing::Create(const LUringOptions& options) noexcept {
  io_uring ring{};
  io_uring_params params = MakeParams(options);

  const int result = io_uring_queue_init_params(options.entries, &ring, &params);
  if (result < 0) {
    return std::unexpected(vexo::base::make_neg_errno(result));
  }

  return LUringRing(ring);
}

io_uring_sqe* LUringRing::GetSqe() noexcept { return io_uring_get_sqe(&ring_); }

base::Result<std::size_t> LUringRing::Submit() noexcept {
  const int result = io_uring_submit(&ring_);
  if (result < 0) {
    return std::unexpected(base::make_neg_errno(result));
  }
  return static_cast<std::size_t>(result);
}

// type -> target CQE.res
// data -> target CQE.user_data
void LUringRing::PrepMsgRing(io_uring_sqe* sqe, int target_ring_fd, std::uint32_t type,
                             std::uint64_t data) noexcept {
  assert(sqe != nullptr);

  io_uring_prep_msg_ring(sqe, target_ring_fd, type, data, IORING_MSG_DATA);
}
}  // namespace vexo::luring
