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
#include "coropact/coro/scheduler.h"
#include "coropact/coro/work.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/net_utils.h"
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
  ConnectAwaiter(EventLoop* loop, net::Endpoint peer) noexcept : loop_(loop), peer_(peer) {}

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
    COROPACT_DCHECK(loop_->IsInLoopThread(), "ConnectAwaiter: wrong EventLoop thread");
    scheduler_ = &coro::Scheduler::RequireCurrent();
    resume_work_.SetHandle(continuation);

    auto fd = net::CreateNonBlockingSocket(peer_.native_family());
    if (!fd.has_value()) {
      result_.SetError(fd.error());
      return false;
    }
    fd_ = *fd;

    int rc = 0;
    do {
      rc = ::connect(fd_, peer_.sock_addr(), peer_.sock_addr_len());
    } while (rc < 0 && errno == EINTR);

    if (rc == 0) {
      result_.SetResult(MakeStream());
      return false;
    }
    if (errno != EINPROGRESS) {
      result_.SetError(base::CurrentErrno());
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
    ReactorStream stream(loop_, fd_, peer_);
    fd_ = -1;
    return stream;
  }

  void OnReady() noexcept {
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
    scheduler_->Schedule(&resume_work_);
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
  int fd_{-1};
  std::optional<Channel> channel_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_{};
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
    COROPACT_DCHECK(loop_->IsInLoopThread(), "SleepAwaiter: wrong EventLoop thread");
    scheduler_ = &coro::Scheduler::RequireCurrent();
    resume_work_.SetHandle(continuation);
    const auto seconds = std::chrono::duration<double>(delay_).count();
    loop_->RunAfter(seconds, [this] { scheduler_->Schedule(&resume_work_); });
    return true;
  }

  void await_resume() const noexcept {}

private:
  EventLoop* loop_;
  std::chrono::milliseconds delay_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_;
};

}  // namespace

ReactorConnector::ReactorConnector(EventLoop* loop) noexcept : loop_(loop) {
  COROPACT_CHECK(loop_ != nullptr, "ReactorConnector: loop must not be null");
}

[[nodiscard]]
base::Result<ReactorConnector> ReactorConnector::Create(EventLoop* loop) noexcept {
  if (loop == nullptr) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  return ReactorConnector(loop);
}

ReactorConnector::ReactorConnector(ReactorConnector&& other) noexcept
    : loop_(std::exchange(other.loop_, nullptr)) {}

ReactorConnector& ReactorConnector::operator=(ReactorConnector&& other) noexcept {
  if (this != &other) {
    loop_ = std::exchange(other.loop_, nullptr);
  }
  return *this;
}

coro::Task<base::Result<ReactorStream>> ReactorConnector::Connect(const net::Endpoint& peer) {
  co_return co_await ConnectAwaiter(loop_, peer);
}

coro::Task<base::Result<ReactorStream>> ReactorConnector::Connect(std::string_view host,
                                                                  std::uint16_t port) {
  COROPACT_CO_TRY(peer, net::ParseIpAddress(host, port));
  co_return co_await Connect(peer);
}

coro::Task<void> ReactorConnector::SleepFor(std::chrono::milliseconds delay) {
  co_await SleepAwaiter(loop_, delay);
}

}  // namespace coropact::reactor
