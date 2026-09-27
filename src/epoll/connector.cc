// SPDX-License-Identifier: MIT
#include "alyrn/epoll/connector.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <expected>
#include <optional>
#include <utility>

#include "alyrn/backend/value_result_state.h"
#include "alyrn/detail/check.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/net/tcp_options.h"
#include "alyrn/detail/scheduler_continuation.h"
#include "alyrn/detail/single_result_lifecycle.h"
#include "alyrn/epoll/detail/channel.h"
#include "alyrn/epoll/detail/loop_access.h"
#include "alyrn/epoll/timer.h"
#include "alyrn/result.h"

namespace alyrn::epoll {
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

class [[nodiscard]] ConnectAwaiter {
public:
  ConnectAwaiter(Loop* loop, net::Endpoint peer, StreamOptions stream_options,
                 net::TcpOptions tcp_options, time::Duration timeout) noexcept
      : loop_(loop),
        peer_(peer),
        stream_options_(stream_options),
        tcp_options_(tcp_options),
        timeout_(timeout) {}

  ~ConnectAwaiter() {
    ALYRN_CHECK(!(channel_.has_value() && channel_->IsRegistered()),
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
    ALYRN_CHECK(loop_ != nullptr, "ConnectAwaiter has no owner Loop");
    ALYRN_CHECK(loop_->IsInLoopThread(), "ConnectAwaiter called from wrong Loop thread");
    if (loop_->State() == backend::LoopState::kStopping ||
        loop_->State() == backend::LoopState::kStopped) {
      CompleteInline(std::unexpected(Errno(ECANCELED)));
      return false;
    }
    continuation_.Bind(continuation);
    LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);

    auto fd = net::CreateNonBlockingSocket(peer_.NativeFamily());
    if (!fd.HasValue()) {
      CompleteInline(std::unexpected(fd.Error()));
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
    if (timeout_ > time::Duration::zero()) {
      timer_ = loop_->RunAfter(timeout_, [this] {
        // The queue already removed this timer before running it.
        timer_ = {};
        CompletePending(std::unexpected(Errno(ETIMEDOUT)));
      });
    }
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
    if (!configured.HasValue()) {
      return std::unexpected(configured.Error());
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
    if (!error.HasValue()) {
      CompletePending(std::unexpected(error.Error()));
    } else if (*error == 0) {
      CompletePending(MakeStream());
    } else {
      CompletePending(std::unexpected(Errno(*error)));
    }
  }

  void CompleteInline(Result<Stream> result) noexcept {
    result_.SetResult(std::move(result));
    ALYRN_CHECK(lifecycle_.TryAuthorizeResult(), "Epoll Connect result was authorized twice");
    ALYRN_CHECK(lifecycle_.TryAuthorizeRelease(),
                   "Epoll Connect release was not authorized after its result");
    ReleasePhysicalRequest();
  }

  void CompletePending(Result<Stream> result) noexcept {
    if (!lifecycle_.TryAuthorizeResult()) {
      return;
    }
    result_.SetResult(std::move(result));
    ALYRN_CHECK(lifecycle_.TryAuthorizeRelease(),
                   "Epoll Connect release was not authorized after its result");
    ReleasePhysicalRequest();
    ALYRN_CHECK(lifecycle_.TryAuthorizeContinuation(),
                   "Epoll Connect continuation was not authorized after release");
    continuation_.Schedule();
  }

  void ReleasePhysicalRequest() noexcept {
    if (timer_.Valid()) {
      loop_->Cancel(std::exchange(timer_, {}));
    }
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
  time::Duration timeout_;
  time::TimerId timer_{};
  int fd_{-1};
  std::optional<Channel> channel_;
  ::alyrn::detail::SchedulerContinuation continuation_;
  ::alyrn::detail::SingleResultLifecycle lifecycle_;
  backend::ValueResultState<Stream> result_;
  LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

coro::Task<Result<Stream>> ConnectResolved(Loop* loop, ConnectorOptions options,
                                           Result<net::Endpoint> peer) {
  if (!peer.HasValue()) {
    co_return std::unexpected(peer.Error());
  }
  co_return co_await ConnectAwaiter(loop, *peer, options.stream_options, options.tcp_options,
                                    options.connect_timeout);
}

}  // namespace

Connector::Connector(Loop* loop, ConnectorOptions options) noexcept
    : loop_(loop), options_(options) {
  ALYRN_CHECK(loop_ != nullptr, "Connector: loop must not be null");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Connector created from wrong Loop thread");
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
  return ConnectResolved(loop_, options_, Result<net::Endpoint>(std::in_place, peer));
}

coro::Task<Result<Stream>> Connector::Connect(std::string_view ip, std::uint16_t port) {
  RequireOwnerLoop();
  return ConnectResolved(loop_, options_, net::ParseIpAddress(ip, port));
}

coro::Task<void> Connector::SleepFor(time::Duration delay) {
  RequireOwnerLoop();
  (void)co_await epoll::SleepFor(*loop_, delay);
}

void Connector::RequireOwnerLoop() const noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Connector operation has no owner Loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Connector operation called from wrong Loop thread");
}

}  // namespace alyrn::epoll
