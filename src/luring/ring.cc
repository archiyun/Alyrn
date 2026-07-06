#include "vexo/luring/ring.h"

#include <liburing.h>
#include <liburing/io_uring.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/luring/options.h"

namespace vexo::luring {

namespace {

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
  if (this == &other) return *this;

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

  const int r = io_uring_queue_init_params(options.entries, &ring, &params);
  if (r < 0) {
    return std::unexpected(vexo::base::make_neg_errno(r));
  }

  return LUringRing(ring);
}

io_uring_sqe* LUringRing::GetSqe() noexcept {
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe != nullptr) return sqe;

  const int submitted = io_uring_submit(&ring_);
  if (submitted < 0) return nullptr;

  return io_uring_get_sqe(&ring_);
}

base::Result<void> LUringRing::Submit() noexcept {
  const int r = io_uring_submit(&ring_);
  if (r < 0) {
    return std::unexpected(base::make_neg_errno(r));
  }
  return {};
}

}  // namespace vexo::luring
