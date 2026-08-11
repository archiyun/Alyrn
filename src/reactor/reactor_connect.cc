// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <coroutine>
#include <expected>
#include <optional>
#include <utility>

#include "coropact/backend/detail/value_result_state.h"
#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/socket.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/operation/detail/single_result_lifecycle.h"
#include "coropact/reactor/connector.h"
#include "coropact/reactor/detail/channel.h"
#include "coropact/reactor/detail/loop_access.h"

namespace coropact::reactor {
namespace {

using namespace detail;

base::Result<int> ConnectError(int fd) noexcept {
  int err = 0;
  auto len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    return std::unexpected(base::CurrentErrno());
  }
  return err;
}

class ConnectAwaiter {
public:
  ConnectAwaiter(EventLoop* loop, net::Endpoint peer, ReactorStreamOptions stream_options) noexcept
      : loop_(loop), peer_(peer), stream_options_(stream_options) {}

  ~ConnectAwaiter() {
    COROPACT_CHECK(!channel_.has_value() || !channel_->IsRegistered(),
                   "ConnectAwaiter destroyed before its physical connect settled");
    if (shutdown_participant_.InList()) {
      LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
    }
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    COROPACT_CHECK(loop_ != nullptr, "ConnectAwaiter has no owner EventLoop");
    COROPACT_CHECK(loop_->IsInLoopThread(), "ConnectAwaiter called from wrong EventLoop thread");
    if (loop_->State() == backend::LoopState::kStopping ||
        loop_->State() == backend::LoopState::kStopped) {
      CompleteInline(std::unexpected(base::MakeErrno(ECANCELED)));
      return false;
    }
    continuation_.Bind(continuation);
    LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);

    auto fd = net::CreateNonBlockingSocket(peer_.native_family());
    if (!fd.has_value()) {
      CompleteInline(std::unexpected(fd.error()));
      return false;
    }
    fd_ = *fd;

    int rc = 0;
    do {
      rc = ::connect(fd_, peer_.sock_addr(), peer_.sock_addr_len());
    } while (rc < 0 && errno == EINTR);

    if (rc == 0) {
      CompleteInline(MakeStream());
      return false;
    }
    if (errno != EINPROGRESS) {
      CompleteInline(std::unexpected(base::CurrentErrno()));
      return false;
    }

    channel_.emplace(loop_, fd_);
    channel_->SetWriteCallback(&ConnectAwaiter::DispatchReady, this);
    channel_->SetErrorCallback(&ConnectAwaiter::DispatchReady, this);
    channel_->EnableWriting();
    return true;
  }

  base::Result<ReactorStream> await_resume() noexcept { return result_.Take(); }

private:
  static void DispatchReady(void* context) noexcept {
    static_cast<ConnectAwaiter*>(context)->OnReady();
  }

  static void DispatchLoopStop(void* context) noexcept {
    auto* self = static_cast<ConnectAwaiter*>(context);
    self->CompletePending(std::unexpected(base::MakeErrno(ECANCELED)));
  }

  base::Result<ReactorStream> MakeStream() noexcept {
    ReactorStream stream(loop_, fd_, peer_, stream_options_);
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
      CompletePending(std::unexpected(base::MakeErrno(*error)));
    }
  }

  void CompleteInline(base::Result<ReactorStream> result) noexcept {
    result_.SetResult(std::move(result));
    COROPACT_CHECK(lifecycle_.TryAuthorizeResult(), "Reactor Connect result was authorized twice");
    COROPACT_CHECK(lifecycle_.TryAuthorizeRelease(),
                   "Reactor Connect release was not authorized after its result");
    ReleasePhysicalRequest();
  }

  void CompletePending(base::Result<ReactorStream> result) noexcept {
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

  EventLoop* loop_;
  net::Endpoint peer_;
  ReactorStreamOptions stream_options_;
  int fd_{-1};
  std::optional<Channel> channel_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::SingleResultLifecycle lifecycle_;
  backend::detail::ValueResultState<ReactorStream> result_;
  LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

coro::Task<base::Result<ReactorStream>> ConnectResolved(EventLoop* loop,
                                                        ReactorStreamOptions stream_options,
                                                        base::Result<net::Endpoint> peer) {
  if (!peer.has_value()) {
    co_return std::unexpected(peer.error());
  }
  co_return co_await ConnectAwaiter(loop, std::move(*peer), stream_options);
}

class SleepAwaiter {
public:
  SleepAwaiter(EventLoop* loop, std::chrono::milliseconds delay) noexcept
      : loop_(loop), delay_(delay) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return delay_.count() <= 0;
  }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    COROPACT_CHECK(loop_ != nullptr, "SleepAwaiter has no owner EventLoop");
    COROPACT_CHECK(loop_->IsInLoopThread(), "SleepAwaiter called from wrong EventLoop thread");
    if (loop_->State() == backend::LoopState::kStopping ||
        loop_->State() == backend::LoopState::kStopped) {
      (void)(completion_gate_.TryComplete());
      return false;
    }
    continuation_.Bind(continuation);
    LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
    const auto seconds = std::chrono::duration<double>(delay_).count();
    timer_ = loop_->RunAfter(seconds, [this] {
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

  EventLoop* loop_;
  std::chrono::milliseconds delay_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  time::TimerId timer_;
  LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

}  // namespace

ReactorConnector::ReactorConnector(EventLoop* loop, ReactorConnectorOptions options) noexcept
    : loop_(loop), options_(options) {
  COROPACT_CHECK(loop_ != nullptr, "ReactorConnector: loop must not be null");
  COROPACT_CHECK(loop_->IsInLoopThread(), "ReactorConnector created from wrong EventLoop thread");
}

[[nodiscard]]
base::Result<ReactorConnector> ReactorConnector::Create(EventLoop* loop,
                                                        ReactorConnectorOptions options) noexcept {
  if (loop == nullptr) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  return ReactorConnector(loop, options);
}

ReactorConnector::ReactorConnector(ReactorConnector&& other) noexcept
    : loop_(std::exchange(other.loop_, nullptr)), options_(other.options_) {}

ReactorConnector& ReactorConnector::operator=(ReactorConnector&& other) noexcept {
  if (this != &other) {
    loop_ = std::exchange(other.loop_, nullptr);
    options_ = other.options_;
  }
  return *this;
}

coro::Task<base::Result<ReactorStream>> ReactorConnector::Connect(net::Endpoint peer) {
  RequireOwnerLoop();
  return ConnectResolved(loop_, options_.stream_options,
                         base::Result<net::Endpoint>(std::in_place, std::move(peer)));
}

coro::Task<base::Result<ReactorStream>> ReactorConnector::Connect(std::string_view host,
                                                                  std::uint16_t port) {
  RequireOwnerLoop();
  return ConnectResolved(loop_, options_.stream_options, net::ParseIpAddress(host, port));
}

coro::Task<void> ReactorConnector::SleepFor(std::chrono::milliseconds delay) {
  RequireOwnerLoop();
  co_await SleepAwaiter(loop_, delay);
}

void ReactorConnector::RequireOwnerLoop() const noexcept {
  COROPACT_CHECK(loop_ != nullptr, "ReactorConnector operation has no owner EventLoop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "ReactorConnector operation called from wrong EventLoop thread");
}

}  // namespace coropact::reactor
