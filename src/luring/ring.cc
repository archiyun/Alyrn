// SPDX-License-Identifier: MIT
#include "coropact/luring/detail/ring.h"

#include <liburing.h>
#include <liburing/io_uring.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/result.h"
#include "coropact/luring/options.h"

namespace coropact::luring::detail {

namespace {

// Convert the high-level LUringOptions into native liburing parameters.
[[nodiscard]]
io_uring_params MakeParams(const LUringOptions& options) noexcept {
  io_uring_params params{};
  params.flags |= IORING_SETUP_CLAMP;

  // One loop owns one ring, so these flags are implementation policy rather
  // than caller configuration. Keeping them fixed makes the submission model
  // uniform across every LUringLoop.
  params.flags |= IORING_SETUP_SUBMIT_ALL;
  params.flags |= IORING_SETUP_SINGLE_ISSUER;

  if (options.setup_sqpoll) {
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = options.sqpoll_idle_ms;
  }

  return params;
}

}  // namespace

LUringRing::~LUringRing() noexcept {
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

Result<LUringRing> LUringRing::Create(const LUringOptions& options) noexcept {
  io_uring ring{};
  io_uring_params params = MakeParams(options);

  const int result = io_uring_queue_init_params(options.entries, &ring, &params);
  if (result < 0) {
    return std::unexpected(coropact::NegErrno(result));
  }

  return LUringRing(ring);
}

io_uring_sqe* LUringRing::GetSqe() noexcept { return io_uring_get_sqe(&ring_); }

Result<std::size_t> LUringRing::Submit() noexcept {
  const int result = io_uring_submit(&ring_);
  if (result < 0) {
    return std::unexpected(NegErrno(result));
  }
  return static_cast<std::size_t>(result);
}

// type -> target CQE.res
// data -> target CQE.user_data
void LUringRing::PrepMsgRing(io_uring_sqe* sqe, int target_ring_fd, std::uint32_t type,
                             std::uint64_t data) noexcept {
  COROPACT_CHECK(sqe != nullptr, "LUringRing::PrepMsgRing received null SQE");

  io_uring_prep_msg_ring(sqe, target_ring_fd, type, data, IORING_MSG_DATA);
}

}  // namespace coropact::luring::detail
