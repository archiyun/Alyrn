// SPDX-License-Identifier: MIT
#include "alyrn/uring/connector.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <expected>
#include <string_view>
#include <utility>

#include "alyrn/backend/value_result_state.h"
#include "alyrn/detail/check.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/uring/detail/completion_dispatch.h"
#include "alyrn/uring/detail/op.h"
#include "alyrn/uring/detail/operation_submission.h"
#include "alyrn/uring/detail/sqe_prep.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/stream.h"
#include "alyrn/uring/timer.h"

namespace alyrn::uring {

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
class [[nodiscard]] ConnectAwaiter : public detail::OpHook<ConnectAwaiter> {
  friend void ::alyrn::uring::detail::DispatchConnectComplete(::alyrn::uring::detail::Op* op) noexcept;

public:
  using OpHook = detail::OpHook<ConnectAwaiter>;

  ConnectAwaiter(Loop* loop, net::Endpoint peer, net::TcpOptions tcp_options) noexcept
      : OpHook(OpKind::kConnect), loop_(loop), peer_(peer), tcp_options_(tcp_options) {}

  ~ConnectAwaiter() noexcept {
    ALYRN_CHECK(!Operation()->resume_work.HasHandle() || Operation()->CqeCompletionRecorded(),
                "ConnectAwaiter destroyed before its physical connect CQE settled");
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    ALYRN_CHECK(loop_ != nullptr, "Connector operation has no owner loop");
    ALYRN_CHECK(loop_->IsInLoopThread(), "Connector operation called from wrong Loop thread");
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
  static void OnComplete(::alyrn::uring::detail::Op* op) noexcept {
    auto* self = OpHook::OwnerFrom(op);
    ALYRN_CHECK(op->result.HasValue(), "Uring Connect CQE is missing its result");

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

    ALYRN_CHECK(op->TryAuthorizeCoupledResult(), "Uring Connect result was authorized twice");
    ALYRN_CHECK(op->TryAuthorizeCoupledRelease(),
                "Uring Connect release was not authorized after its result");
    self->ReleasePhysicalRequest();
  }

  void CompleteInline(Result<Stream> result) noexcept {
    result_.SetResult(std::move(result));
    ALYRN_CHECK(Operation()->TryAuthorizeCoupledResult(),
                "Uring Connect result was authorized twice");
    ALYRN_CHECK(Operation()->TryAuthorizeCoupledRelease(),
                "Uring Connect release was not authorized after its result");
    ReleasePhysicalRequest();
  }

  Result<Stream> MakeStream() noexcept {
    auto configured = net::ApplyTcpOptions(fd_, tcp_options_);
    if (!configured.has_value()) {
      return std::unexpected(configured.error());
    }
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
  net::TcpOptions tcp_options_;
  int fd_{-1};
  backend::ValueResultState<Stream> result_;
};

coro::Task<Result<Stream>> ConnectResolved(Loop* loop, net::TcpOptions tcp_options,
                                           Result<net::Endpoint> peer) {
  if (!peer.has_value()) {
    co_return std::unexpected(peer.error());
  }
  co_return co_await ConnectAwaiter(loop, *peer, tcp_options);
}

}  // namespace

namespace detail {

void DispatchConnectComplete(Op* op) noexcept {
  ConnectAwaiter::OnComplete(op);
}

}  // namespace detail

Connector::Connector(Loop* loop, ConnectorOptions options) noexcept
    : loop_(loop), options_(options) {
  ALYRN_CHECK(loop_ != nullptr, "Connector: loop must not be null");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Connector created from wrong Loop thread");
}

Result<Connector> Connector::Create(Loop* loop, ConnectorOptions options) noexcept {
  if (loop == nullptr) {
    return std::unexpected(Errno(EINVAL));
  }
  return Connector{loop, options};
}

Connector::Connector(Connector&& other) noexcept
    : loop_(std::exchange(other.loop_, nullptr)), options_(std::exchange(other.options_, {})) {}

Connector& Connector::operator=(Connector&& other) noexcept {
  if (this != &other) {
    loop_ = std::exchange(other.loop_, nullptr);
    options_ = std::exchange(other.options_, {});
  }
  return *this;
}

coro::Task<Result<Stream>> Connector::Connect(std::string_view host, std::uint16_t port) {
  RequireOwnerLoop();
  return ConnectResolved(loop_, options_.tcp_options, net::ParseIpAddress(host, port));
}

coro::Task<void> Connector::SleepFor(time::Duration delay) {
  RequireOwnerLoop();
  auto result = co_await uring::SleepFor(*loop_, delay);
  (void)result;
}

void Connector::RequireOwnerLoop() const noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Connector operation has no owner Loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Connector operation called from wrong Loop thread");
}

}  // namespace alyrn::uring
