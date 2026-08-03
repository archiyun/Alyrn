// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/reactor/reactor_connect.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <coroutine>
#include <expected>
#include <optional>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/base/try.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/net_utils.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/reactor/channel.h"

namespace coropact::reactor {
namespace {

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
  ConnectAwaiter(EventLoop* loop, net::Endpoint peer,
                 ReactorStreamOptions stream_options) noexcept
      : loop_(loop), peer_(peer), stream_options_(stream_options) {}

  ~ConnectAwaiter() {
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
    continuation_.Bind(continuation);

    auto fd = net::CreateNonBlockingSocket(peer_.native_family());
    if (!fd.has_value()) {
      result_.SetError(fd.error());
      COROPACT_IGNORE_RESULT(completion_gate_.TryComplete());
      return false;
    }
    fd_ = *fd;

    int rc = 0;
    do {
      rc = ::connect(fd_, peer_.sock_addr(), peer_.sock_addr_len());
    } while (rc < 0 && errno == EINTR);

    if (rc == 0) {
      result_.SetResult(MakeStream());
      COROPACT_IGNORE_RESULT(completion_gate_.TryComplete());
      return false;
    }
    if (errno != EINPROGRESS) {
      result_.SetError(base::CurrentErrno());
      COROPACT_IGNORE_RESULT(completion_gate_.TryComplete());
      return false;
    }

    channel_.emplace(loop_, fd_);
    channel_->SetWriteCallback(&ConnectAwaiter::DispatchReady, this);
    channel_->SetErrorCallback(&ConnectAwaiter::DispatchReady, this);
    channel_->EnableWriting();
    return true;
  }

  base::Result<ReactorStream> await_resume() noexcept {
    COROPACT_DCHECK(result_.HasResult(), "ConnectAwaiter: result is not ready");
    return result_.Take();
  }

private:
  static void DispatchReady(void* context) noexcept {
    static_cast<ConnectAwaiter*>(context)->OnReady();
  }

  base::Result<ReactorStream> MakeStream() noexcept {
    DetachChannel();
    ReactorStream stream(loop_, fd_, peer_, stream_options_);
    fd_ = -1;
    return stream;
  }

  void OnReady() noexcept {
    if (!completion_gate_.TryComplete()) {
      return;
    }
    auto error = ConnectError(fd_);
    if (!error.has_value()) {
      DetachChannel();
      result_.SetError(error.error());
    } else if (*error == 0) {
      result_.SetResult(MakeStream());
    } else {
      DetachChannel();
      result_.SetError(base::MakeErrno(*error));
    }
    continuation_.Schedule();
  }

  void DetachChannel() noexcept {
    if (!channel_) return;
    if (!channel_->IsNoneEvent()) {
      channel_->DisableAll();
    }
    if (loop_->HasChannel(&*channel_)) {
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
  operation::detail::CompletionGate completion_gate_;
  detail::ReactorValueResultState<ReactorStream> result_;
};

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
    continuation_.Bind(continuation);
    const auto seconds = std::chrono::duration<double>(delay_).count();
    loop_->RunAfter(seconds, [this] {
      if (completion_gate_.TryComplete()) {
        continuation_.Schedule();
      }
    });
    return true;
  }

  void await_resume() const noexcept {}

private:
  EventLoop* loop_;
  std::chrono::milliseconds delay_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
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

coro::Task<base::Result<ReactorStream>> ReactorConnector::Connect(const net::Endpoint& peer) {
  RequireOwnerLoop();
  co_return co_await ConnectAwaiter(loop_, peer, options_.stream_options);
}

coro::Task<base::Result<ReactorStream>> ReactorConnector::Connect(std::string_view host,
                                                                  std::uint16_t port) {
  COROPACT_CO_TRY(peer, net::ParseIpAddress(host, port));
  co_return co_await Connect(peer);
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
