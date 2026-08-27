// SPDX-License-Identifier: MIT
#include "coropact/reactor/connector.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <expected>
#include <optional>
#include <utility>

#include "coropact/backend/detail/value_result_state.h"
#include "coropact/base/check.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/socket.h"
#include "coropact/net/tcp_options.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/operation/detail/single_result_lifecycle.h"
#include "coropact/reactor/detail/channel.h"
#include "coropact/reactor/detail/loop_access.h"
#include "coropact/result.h"

namespace coropact::reactor {
namespace {

using namespace detail;

Result<int> ConnectError(int fd) noexcept {
  int err = 0;
  auto len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    return std::unexpected(CurrentErrno());
  }
  return err;
}

class ConnectAwaiter {
public:
  ConnectAwaiter(Loop* loop, net::Endpoint peer, StreamOptions stream_options,
                 net::TcpOptions tcp_options) noexcept
      : loop_(loop), peer_(peer), stream_options_(stream_options), tcp_options_(tcp_options) {}

  ~ConnectAwaiter() {
    COROPACT_CHECK(!(channel_.has_value() && channel_->IsRegistered()),
                   "ConnectAwaiter destroyed before its physical connect settled");
    if (shutdown_participant_.InList()) {
      LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
    }
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    COROPACT_CHECK(loop_ != nullptr, "ConnectAwaiter has no owner Loop");
    COROPACT_CHECK(loop_->IsInLoopThread(), "ConnectAwaiter called from wrong Loop thread");
    if (loop_->State() == backend::LoopState::kStopping ||
        loop_->State() == backend::LoopState::kStopped) {
      CompleteInline(std::unexpected(Errno(ECANCELED)));
      return false;
    }
    continuation_.Bind(continuation);
    LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);

    auto fd = net::CreateNonBlockingSocket(peer_.NativeFamily());
    if (!fd.has_value()) {
      CompleteInline(std::unexpected(fd.error()));
      return false;
    }
    fd_ = *fd;

    int rc = 0;
    do {
      rc = ::connect(fd_, peer_.SockAddr(), peer_.SockAddrLen());
    } while (rc < 0 && errno == EINTR);

    if (rc == 0) {
      CompleteInline(MakeStream());
      return false;
    }
    if (errno != EINPROGRESS) {
      CompleteInline(std::unexpected(CurrentErrno()));
      return false;
    }

    channel_.emplace(loop_, fd_);
    channel_->SetWriteCallback(&ConnectAwaiter::DispatchReady, this);
    channel_->SetErrorCallback(&ConnectAwaiter::DispatchReady, this);
    channel_->EnableWriting();
    return true;
  }

  Result<Stream> await_resume() noexcept { return result_.Take(); }

private:
  static void DispatchReady(void* context) noexcept {
    static_cast<ConnectAwaiter*>(context)->OnReady();
  }

  static void DispatchLoopStop(void* context) noexcept {
    auto* self = static_cast<ConnectAwaiter*>(context);
    self->CompletePending(std::unexpected(Errno(ECANCELED)));
  }

  Result<Stream> MakeStream() noexcept {
    auto configured = net::ApplyTcpOptions(fd_, tcp_options_);
    if (!configured.has_value()) {
      return std::unexpected(configured.error());
    }
    Stream stream(loop_, fd_, peer_, stream_options_);
    fd_ = -1;
    return stream;
  }

  void OnReady() noexcept {
    if (lifecycle_.ResultReady()) {
      return;
    }
    auto error = ConnectError(fd_);
    if (!error.has_value()) {
      CompletePending(std::unexpected(error.error()));
    } else if (*error == 0) {
      CompletePending(MakeStream());
    } else {
      CompletePending(std::unexpected(Errno(*error)));
    }
  }

  void CompleteInline(Result<Stream> result) noexcept {
    result_.SetResult(std::move(result));
    COROPACT_CHECK(lifecycle_.TryAuthorizeResult(), "Reactor Connect result was authorized twice");
    COROPACT_CHECK(lifecycle_.TryAuthorizeRelease(),
                   "Reactor Connect release was not authorized after its result");
    ReleasePhysicalRequest();
  }

  void CompletePending(Result<Stream> result) noexcept {
    if (!lifecycle_.TryAuthorizeResult()) {
      return;
    }
    result_.SetResult(std::move(result));
    COROPACT_CHECK(lifecycle_.TryAuthorizeRelease(),
                   "Reactor Connect release was not authorized after its result");
    ReleasePhysicalRequest();
    COROPACT_CHECK(lifecycle_.TryAuthorizeContinuation(),
                   "Reactor Connect continuation was not authorized after release");
    continuation_.Schedule();
  }

  void ReleasePhysicalRequest() noexcept {
    DetachChannel();
    if (shutdown_participant_.InList()) {
      LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
    }
    if (fd_ >= 0) {
      (void)::close(std::exchange(fd_, -1));
    }
  }

  void DetachChannel() noexcept {
    if (!channel_) return;
    if (!channel_->IsNoneEvent()) {
      channel_->DisableAll();
    }
    if (channel_->IsRegistered()) {
      channel_->Remove();
    }
    channel_.reset();
  }

  Loop* loop_;
  net::Endpoint peer_;
  StreamOptions stream_options_;
  net::TcpOptions tcp_options_;
  int fd_{-1};
  std::optional<Channel> channel_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::SingleResultLifecycle lifecycle_;
  backend::detail::ValueResultState<Stream> result_;
  LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

coro::Task<Result<Stream>> ConnectResolved(Loop* loop, StreamOptions stream_options,
                                           net::TcpOptions tcp_options,
                                           Result<net::Endpoint> peer) {
  if (!peer.has_value()) {
    co_return std::unexpected(peer.error());
  }
  co_return co_await ConnectAwaiter(loop, *peer, stream_options, tcp_options);
}

class SleepAwaiter {
public:
  SleepAwaiter(Loop* loop, time::Duration delay) noexcept : loop_(loop), delay_(delay) {}

  bool await_ready() const noexcept { return delay_ <= time::Duration::zero(); }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    COROPACT_CHECK(loop_ != nullptr, "SleepAwaiter has no owner Loop");
    COROPACT_CHECK(loop_->IsInLoopThread(), "SleepAwaiter called from wrong Loop thread");
    if (loop_->State() == backend::LoopState::kStopping ||
        loop_->State() == backend::LoopState::kStopped) {
      (void)(completion_gate_.TryComplete());
      return false;
    }
    continuation_.Bind(continuation);
    LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
    timer_ = loop_->RunAfter(delay_, [this] {
      if (completion_gate_.TryComplete()) {
        continuation_.Schedule();
      }
    });
    return true;
  }

  void await_resume() const noexcept {}

  ~SleepAwaiter() {
    if (shutdown_participant_.InList()) {
      LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
    }
  }

private:
  static void DispatchLoopStop(void* context) noexcept {
    auto* self = static_cast<SleepAwaiter*>(context);
    if (!self->completion_gate_.TryComplete()) {
      return;
    }
    if (self->timer_.Valid()) {
      self->loop_->Cancel(self->timer_);
      self->timer_ = {};
    }
    self->continuation_.Schedule();
  }

  Loop* loop_;
  time::Duration delay_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  time::TimerId timer_;
  LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

}  // namespace

Connector::Connector(Loop* loop, ConnectorOptions options) noexcept
    : loop_(loop), options_(options) {
  COROPACT_CHECK(loop_ != nullptr, "Connector: loop must not be null");
  COROPACT_CHECK(loop_->IsInLoopThread(), "Connector created from wrong Loop thread");
}

[[nodiscard]]
Result<Connector> Connector::Create(Loop* loop, ConnectorOptions options) noexcept {
  if (loop == nullptr) {
    return std::unexpected(Errno(EINVAL));
  }
  return Connector(loop, options);
}

Connector::Connector(Connector&& other) noexcept
    : loop_(std::exchange(other.loop_, nullptr)), options_(other.options_) {}

Connector& Connector::operator=(Connector&& other) noexcept {
  if (this != &other) {
    loop_ = std::exchange(other.loop_, nullptr);
    options_ = other.options_;
  }
  return *this;
}

coro::Task<Result<Stream>> Connector::Connect(net::Endpoint peer) {
  RequireOwnerLoop();
  return ConnectResolved(loop_, options_.stream_options, options_.tcp_options,
                         Result<net::Endpoint>(std::in_place, peer));
}

coro::Task<Result<Stream>> Connector::Connect(std::string_view host, std::uint16_t port) {
  RequireOwnerLoop();
  return ConnectResolved(loop_, options_.stream_options, options_.tcp_options,
                         net::ParseIpAddress(host, port));
}

coro::Task<void> Connector::SleepFor(time::Duration delay) {
  RequireOwnerLoop();
  co_await SleepAwaiter(loop_, delay);
}

void Connector::RequireOwnerLoop() const noexcept {
  COROPACT_CHECK(loop_ != nullptr, "Connector operation has no owner Loop");
  COROPACT_CHECK(loop_->IsInLoopThread(), "Connector operation called from wrong Loop thread");
}

}  // namespace coropact::reactor
