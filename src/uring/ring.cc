// SPDX-License-Identifier: MIT
#include "alyrn/detail/uring/ring.h"

#include <liburing.h>
#include <liburing/io_uring.h>

#include <cstdlib>
#include <cstring>
#include <expected>
#include <utility>

#include "alyrn/result.h"
#include "alyrn/uring/options.h"

namespace alyrn::uring::detail {

namespace {

// Convert the high-level Options into native liburing parameters.
[[nodiscard]]
io_uring_params MakeParams(const Options& options) noexcept {
  io_uring_params params{};
  params.flags |= IORING_SETUP_CLAMP;

  // One loop owns one ring, so these flags are implementation policy rather
  // than caller configuration. Keeping them fixed makes the submission model
  // uniform across every Loop.
  params.flags |= IORING_SETUP_SUBMIT_ALL;
  params.flags |= IORING_SETUP_SINGLE_ISSUER;

  if (options.setup_sqpoll) {
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = options.sqpoll_idle_ms;
  }

  return params;
}

}  // namespace

Ring::~Ring() noexcept {
  if (initialized_) {
    io_uring_queue_exit(&ring_);
  }
}

Ring::Ring(Ring&& other) noexcept
    : ring_(other.ring_), initialized_(std::exchange(other.initialized_, false)) {
  std::memset(&other.ring_, 0, sizeof(other.ring_));
}

Ring& Ring::operator=(Ring&& other) noexcept {
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

Result<Ring> Ring::Create(const Options& options) noexcept {
  io_uring ring{};
  io_uring_params params = MakeParams(options);

  const int result = io_uring_queue_init_params(options.entries, &ring, &params);
  if (result < 0) {
    return std::unexpected(alyrn::NegErrno(result));
  }

  return Ring(ring);
}

io_uring_sqe* Ring::GetSqe() noexcept { return io_uring_get_sqe(&ring_); }

Result<std::size_t> Ring::Submit() noexcept {
  const int result = io_uring_submit(&ring_);
  if (result < 0) {
    return std::unexpected(NegErrno(result));
  }
  return static_cast<std::size_t>(result);
}

}  // namespace alyrn::uring::detail
