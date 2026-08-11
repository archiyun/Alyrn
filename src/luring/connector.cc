// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/luring/connector.h"

#include <fcntl.h>
#include <liburing.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <expected>
#include <string_view>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/base/try.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/detail/operation_submission.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/stream.h"
#include "coropact/luring/timer.h"
#include "coropact/net/endpoint.h"

namespace coropact::luring {

using namespace detail;

namespace {

base::Result<int> CreateSocket(sa_family_t family) noexcept {
  const int fd = ::socket(family, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(base::CurrentErrno());
  }
  return fd;
}

base::Result<void> SetNonBlocking(int fd) noexcept {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return std::unexpected(base::CurrentErrno());
  }
  if ((flags & O_NONBLOCK) != 0) {
    return {};
  }
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return std::unexpected(base::CurrentErrno());
  }
  return {};
}

// --- ConnectAwaiter ---
class ConnectAwaiter : public detail::LUringOpHook<ConnectAwaiter> {
public:
  using OpHook = detail::LUringOpHook<ConnectAwaiter>;

  ConnectAwaiter(LUringLoop* loop, net::Endpoint peer) noexcept
      : OpHook(LUringOpKind::kConnect), loop_(loop), peer_(std::move(peer)) {}

  ~ConnectAwaiter() noexcept {
    COROPACT_CHECK(!Op()->resume_work.HasHandle() || Op()->CqeCompletionRecorded(),
                   "ConnectAwaiter destroyed before its physical connect CQE settled");
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    COROPACT_CHECK(loop_ != nullptr, "LUringConnector operation has no owner loop");
    COROPACT_CHECK(loop_->IsInLoopThread(),
                   "LUringConnector operation called from wrong LUringLoop thread");
    if (loop_->State() == backend::LoopState::kStopping ||
        loop_->State() == backend::LoopState::kStopped) {
      Op()->SetImmediateError(base::MakeErrno(ECANCELED));
      return false;
    }

    auto fd = CreateSocket(peer_.NativeFamily());
    if (!fd.has_value()) {
      Op()->SetImmediateError(fd.error());
      return false;
    }
    fd_ = *fd;

    Op()->kind = LUringOpKind::kConnect;
    return detail::SubmitAwaitingOperation(
        *loop_, *Op(), continuation,
        [this, fd = fd_](io_uring_sqe* sqe) noexcept {
          io_uring_prep_connect(sqe, fd, peer_.SockAddr(), peer_.SockAddrLen());
        },
        [this](base::Error error) noexcept { Op()->SetImmediateError(error); });
  }

  base::Result<LUringStream> await_resume() noexcept {
    if (!Op()->CqeCompletionRecorded()) {
      COROPACT_CHECK(Op()->result.HasValue(),
                     "LUring Connect resumed before its immediate result was ready");
      return std::unexpected(base::MakeNegErrno(*Op()->result));
    }

    COROPACT_CHECK(Op()->result.HasValue(), "LUring Connect CQE is missing its result");
    if (*Op()->result < 0) {
      return std::unexpected(base::MakeNegErrno(*Op()->result));
    }

    COROPACT_TRY(SetNonBlocking(fd_));

    LUringStream stream(loop_, fd_, peer_);
    fd_ = -1;
    return stream;
  }

private:
  LUringLoop* loop_;
  net::Endpoint peer_;
  int fd_{-1};
};

coro::Task<base::Result<LUringStream>> ConnectResolved(LUringLoop* loop,
                                                       base::Result<net::Endpoint> peer) {
  if (!peer.has_value()) {
    co_return std::unexpected(peer.error());
  }
  co_return co_await ConnectAwaiter(loop, std::move(*peer));
}

}  // namespace

LUringConnector::LUringConnector(LUringLoop* loop) noexcept : loop_(loop) {
  COROPACT_CHECK(loop_ != nullptr, "LUringConnector: loop must not be null");
  COROPACT_CHECK(loop_->IsInLoopThread(), "LUringConnector created from wrong LUringLoop thread");
}

base::Result<LUringConnector> LUringConnector::Create(LUringLoop* loop) noexcept {
  if (loop == nullptr) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  return LUringConnector{loop};
}

LUringConnector::LUringConnector(LUringConnector&& other) noexcept
    : loop_(std::exchange(other.loop_, nullptr)) {}

LUringConnector& LUringConnector::operator=(LUringConnector&& other) noexcept {
  if (this != &other) {
    loop_ = std::exchange(other.loop_, nullptr);
  }
  return *this;
}

coro::Task<base::Result<LUringStream>> LUringConnector::Connect(std::string_view host,
                                                                std::uint16_t port) {
  RequireOwnerLoop();
  return ConnectResolved(loop_, net::ParseIpAddress(host, port));
}

coro::Task<void> LUringConnector::SleepFor(std::chrono::milliseconds delay) {
  RequireOwnerLoop();
  auto result = co_await coropact::luring::SleepFor(*loop_, delay);
  (void)result;
}

void LUringConnector::RequireOwnerLoop() const noexcept {
  COROPACT_CHECK(loop_ != nullptr, "LUringConnector operation has no owner LUringLoop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "LUringConnector operation called from wrong LUringLoop thread");
}

}  // namespace coropact::luring
