// SPDX-License-Identifier: MIT
#include "coropact/luring/connector.h"

#include <fcntl.h>
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
#include "coropact/luring/detail/sqe_prep.h"
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
class ConnectAwaiter : public detail::OpHook<ConnectAwaiter> {
  friend void detail::DispatchConnectComplete(::coropact::luring::detail::Op* op) noexcept;

public:
  using OpHook = detail::OpHook<ConnectAwaiter>;

  ConnectAwaiter(Loop* loop, net::Endpoint peer) noexcept
      : OpHook(OpKind::kConnect), loop_(loop), peer_(std::move(peer)) {}

  ~ConnectAwaiter() noexcept {
    COROPACT_CHECK(!Operation()->resume_work.HasHandle() || Operation()->CqeCompletionRecorded(),
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
    COROPACT_CHECK(loop_ != nullptr, "Connector operation has no owner loop");
    COROPACT_CHECK(loop_->IsInLoopThread(),
                   "Connector operation called from wrong Loop thread");
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

    Operation()->kind = OpKind::kConnect;
    return detail::SubmitAwaitingOperation(
        *loop_, *Operation(), continuation,
        detail::PrepareConnect(fd_, peer_.SockAddr(), peer_.SockAddrLen()),
        [this](Error error) noexcept { CompleteInline(std::unexpected(error)); });
  }

  Result<Stream> await_resume() noexcept { return result_.Take(); }

private:
  static void OnComplete(::coropact::luring::detail::Op* op) noexcept {
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

  void CompleteInline(Result<Stream> result) noexcept {
    result_.SetResult(std::move(result));
    COROPACT_CHECK(Operation()->TryAuthorizeCoupledResult(), "LUring Connect result was authorized twice");
    COROPACT_CHECK(Operation()->TryAuthorizeCoupledRelease(),
                   "LUring Connect release was not authorized after its result");
    ReleasePhysicalRequest();
  }

  Result<Stream> MakeStream() noexcept {
    Stream stream(loop_, fd_, peer_);
    fd_ = -1;
    return stream;
  }

  void ReleasePhysicalRequest() noexcept {
    if (fd_ >= 0) {
      (void)::close(std::exchange(fd_, -1));
    }
  }

  Loop* loop_;
  net::Endpoint peer_;
  int fd_{-1};
  backend::detail::ValueResultState<Stream> result_;
};

coro::Task<Result<Stream>> ConnectResolved(Loop* loop,
                                                       Result<net::Endpoint> peer) {
  if (!peer.has_value()) {
    co_return std::unexpected(peer.error());
  }
  co_return co_await ConnectAwaiter(loop, std::move(*peer));
}

}  // namespace

namespace detail {

void DispatchConnectComplete(::coropact::luring::detail::Op* op) noexcept { ConnectAwaiter::OnComplete(op); }

}  // namespace detail

Connector::Connector(Loop* loop) noexcept : loop_(loop) {
  COROPACT_CHECK(loop_ != nullptr, "Connector: loop must not be null");
  COROPACT_CHECK(loop_->IsInLoopThread(), "Connector created from wrong Loop thread");
}

Result<Connector> Connector::Create(Loop* loop) noexcept {
  if (loop == nullptr) {
    return std::unexpected(Errno(EINVAL));
  }
  return Connector{loop};
}

Connector::Connector(Connector&& other) noexcept
    : loop_(std::exchange(other.loop_, nullptr)) {}

Connector& Connector::operator=(Connector&& other) noexcept {
  if (this != &other) {
    loop_ = std::exchange(other.loop_, nullptr);
  }
  return *this;
}

coro::Task<Result<Stream>> Connector::Connect(std::string_view host,
                                                                std::uint16_t port) {
  RequireOwnerLoop();
  return ConnectResolved(loop_, net::ParseIpAddress(host, port));
}

coro::Task<void> Connector::SleepFor(time::Duration delay) {
  RequireOwnerLoop();
  auto result = co_await coropact::luring::SleepFor(*loop_, delay);
  (void)result;
}

void Connector::RequireOwnerLoop() const noexcept {
  COROPACT_CHECK(loop_ != nullptr, "Connector operation has no owner Loop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "Connector operation called from wrong Loop thread");
}

}  // namespace coropact::luring
