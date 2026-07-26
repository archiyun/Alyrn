// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/luring/connector.h"

#include <fcntl.h>
#include <liburing.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <coroutine>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/op.h"
#include "coropact/luring/stream.h"
#include "coropact/luring/timer.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/net_utils.h"

namespace coropact::luring {

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

class ConnectAwaiter {
public:
  ConnectAwaiter(LUringLoop* loop, net::Endpoint peer) noexcept
      : loop_(loop), peer_(std::move(peer)) {}

  ~ConnectAwaiter() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    assert(loop_ != nullptr);
    assert(loop_->IsInLoopThread());

    auto fd = CreateSocket(peer_.native_family());
    if (!fd.has_value()) {
      result_ = std::unexpected(fd.error());
      return false;
    }
    fd_ = *fd;

    op_.kind = LUringOpKind::kConnect;
    op_.resume_work.SetHandle(continuation);
    op_.owner = this;

    auto submitted = loop_->SubmitOp(&op_, [this, fd = fd_](io_uring_sqe* sqe) noexcept {
      io_uring_prep_connect(sqe, fd, peer_.sock_addr(), peer_.sock_addr_len());
    });
    if (!submitted.has_value()) {
      result_ = std::unexpected(submitted.error());
      return false;
    }

    return true;
  }

  base::Result<LUringStream> await_resume() noexcept {
    if (result_.has_value()) {
      return std::move(*result_);
    }

    assert(op_.completed);
    if (!op_.result.has_value()) {
      return std::unexpected(op_.result.error());
    }
    if (*op_.result < 0) {
      return std::unexpected(base::make_neg_errno(*op_.result));
    }

    auto nonblocking = SetNonBlocking(fd_);
    if (!nonblocking.has_value()) {
      return std::unexpected(nonblocking.error());
    }

    LUringStream stream(loop_, fd_, peer_);
    fd_ = -1;
    return stream;
  }

private:
  LUringLoop* loop_;
  net::Endpoint peer_;
  int fd_{-1};
  LUringOp op_{.kind = LUringOpKind::kConnect};
  std::optional<base::Result<LUringStream>> result_;
};

}  // namespace

LUringConnector::LUringConnector(LUringLoop* loop) noexcept : loop_(loop) {}

base::Result<LUringConnector> LUringConnector::Create(LUringLoop* loop) noexcept {
  if (loop == nullptr) {
    return std::unexpected(base::make_errno(EINVAL));
  }
  return LUringConnector{loop};
}

LUringConnector::LUringConnector(LUringConnector&& other) noexcept : loop_(other.loop_) {
  other.loop_ = nullptr;
}

LUringConnector& LUringConnector::operator=(LUringConnector&& other) noexcept {
  if (this != &other) {
    loop_ = std::exchange(other.loop_, nullptr);
  }
  return *this;
}

coro::Task<base::Result<LUringStream>> LUringConnector::Connect(std::string_view host,
                                                                std::uint16_t port) {
  auto peer = net::ParseIpAddress(host, port);
  if (!peer.has_value()) {
    co_return std::unexpected(peer.error());
  }

  co_return co_await ConnectAwaiter(loop_, *peer);
}

coro::Task<void> LUringConnector::SleepFor(std::chrono::milliseconds delay) {
  auto result = co_await coropact::luring::SleepFor(*loop_, delay);
  (void)result;
}

}  // namespace coropact::luring
