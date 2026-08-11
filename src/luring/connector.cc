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

#include "coropact/backend/detail/value_result_state.h"
#include "coropact/base/check.h"
#include "coropact/result.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/detail/operation_submission.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/stream.h"
#include "coropact/luring/timer.h"
#include "coropact/net/endpoint.h"

namespace coropact::luring {

using namespace detail;

namespace {

Result<int> CreateSocket(sa_family_t family) noexcept {
  const int fd = ::socket(family, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(CurrentErrno());
  }
  return fd;
}

Result<void> SetNonBlocking(int fd) noexcept {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return std::unexpected(CurrentErrno());
  }
  if ((flags & O_NONBLOCK) != 0) {
    return {};
  }
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return std::unexpected(CurrentErrno());
  }
  return {};
}

// --- ConnectAwaiter ---
class ConnectAwaiter : public detail::LUringOpHook<ConnectAwaiter> {
  friend void detail::DispatchConnectComplete(LUringOp* op) noexcept;

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
      CompleteInline(std::unexpected(Errno(ECANCELED)));
      return false;
    }

    auto fd = CreateSocket(peer_.NativeFamily());
    if (!fd.has_value()) {
      CompleteInline(std::unexpected(fd.error()));
      return false;
    }
    fd_ = *fd;

    Op()->kind = LUringOpKind::kConnect;
    return detail::SubmitAwaitingOperation(
        *loop_, *Op(), continuation,
        [this, fd = fd_](io_uring_sqe* sqe) noexcept {
          io_uring_prep_connect(sqe, fd, peer_.SockAddr(), peer_.SockAddrLen());
        },
        [this](Error error) noexcept { CompleteInline(std::unexpected(error)); });
  }

  Result<LUringStream> await_resume() noexcept { return result_.Take(); }

private:
  static void OnComplete(LUringOp* op) noexcept {
    auto* self = OpHook::OwnerFrom(op);
    COROPACT_CHECK(op->result.HasValue(), "LUring Connect CQE is missing its result");

    if (*op->result < 0) {
      self->result_.SetError(NegErrno(*op->result));
    } else {
      auto nonblocking = SetNonBlocking(self->fd_);
      if (!nonblocking.has_value()) {
        self->result_.SetError(nonblocking.error());
      } else {
        self->result_.SetResult(self->MakeStream());
      }
    }

    COROPACT_CHECK(op->TryAuthorizeCoupledResult(), "LUring Connect result was authorized twice");
    COROPACT_CHECK(op->TryAuthorizeCoupledRelease(),
                   "LUring Connect release was not authorized after its result");
    self->ReleasePhysicalRequest();
  }

  void CompleteInline(Result<LUringStream> result) noexcept {
    result_.SetResult(std::move(result));
    COROPACT_CHECK(Op()->TryAuthorizeCoupledResult(), "LUring Connect result was authorized twice");
    COROPACT_CHECK(Op()->TryAuthorizeCoupledRelease(),
                   "LUring Connect release was not authorized after its result");
    ReleasePhysicalRequest();
  }

  Result<LUringStream> MakeStream() noexcept {
    LUringStream stream(loop_, fd_, peer_);
    fd_ = -1;
    return stream;
  }

  void ReleasePhysicalRequest() noexcept {
    if (fd_ >= 0) {
      (void)::close(std::exchange(fd_, -1));
    }
  }

  LUringLoop* loop_;
  net::Endpoint peer_;
  int fd_{-1};
  backend::detail::ValueResultState<LUringStream> result_;
};

coro::Task<Result<LUringStream>> ConnectResolved(LUringLoop* loop,
                                                       Result<net::Endpoint> peer) {
  if (!peer.has_value()) {
    co_return std::unexpected(peer.error());
  }
  co_return co_await ConnectAwaiter(loop, std::move(*peer));
}

}  // namespace

namespace detail {

void DispatchConnectComplete(LUringOp* op) noexcept { ConnectAwaiter::OnComplete(op); }

}  // namespace detail

LUringConnector::LUringConnector(LUringLoop* loop) noexcept : loop_(loop) {
  COROPACT_CHECK(loop_ != nullptr, "LUringConnector: loop must not be null");
  COROPACT_CHECK(loop_->IsInLoopThread(), "LUringConnector created from wrong LUringLoop thread");
}

Result<LUringConnector> LUringConnector::Create(LUringLoop* loop) noexcept {
  if (loop == nullptr) {
    return std::unexpected(Errno(EINVAL));
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

coro::Task<Result<LUringStream>> LUringConnector::Connect(std::string_view host,
                                                                std::uint16_t port) {
  RequireOwnerLoop();
  return ConnectResolved(loop_, net::ParseIpAddress(host, port));
}

coro::Task<void> LUringConnector::SleepFor(time::Duration delay) {
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
