// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/reactor_connect.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <coroutine>
#include <expected>
#include <optional>
#include <utility>

#include "vexo/base/check.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/work.h"
#include "vexo/net/channel.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/net_utils.h"

namespace vexo::net {
namespace {

base::Result<int> ConnectError(int fd) noexcept {
  int err = 0;
  socklen_t len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    return std::unexpected(base::CurrentErrno());
  }
  return err;
}

class ConnectAwaiter {
public:
  ConnectAwaiter(EventLoop* loop, InetAddress peer) noexcept
      : loop_(loop), peer_(std::move(peer)) {}

  ~ConnectAwaiter() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    VEXO_DCHECK(loop_->IsInLoopThread(), "ConnectAwaiter: wrong EventLoop thread");
    scheduler_ = &coro::Scheduler::RequireCurrent();
    resume_work_.handle = continuation;

    auto fd = CreateNonBlockingSocket();
    if (!fd.has_value()) {
      result_ = std::unexpected(fd.error());
      return false;
    }
    fd_ = *fd;

    int rc = 0;
    do {
      rc = ::connect(fd_, reinterpret_cast<const sockaddr*>(&peer_.sock_addr()),
                     sizeof(sockaddr_in));
    } while (rc < 0 && errno == EINTR);

    if (rc == 0) {
      result_ = MakeStream();
      return false;
    }
    if (errno != EINPROGRESS) {
      result_ = std::unexpected(base::CurrentErrno());
      return false;
    }

    channel_.emplace(loop_, fd_);
    channel_->set_write_callback([this] { OnReady(); });
    channel_->set_error_callback([this] { OnReady(); });
    channel_->EnableWriting();
    return true;
  }

  base::Result<ReactorStream> await_resume() noexcept {
    VEXO_DCHECK(result_.has_value(), "ConnectAwaiter: result is not ready");
    return std::move(*result_);
  }

private:
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
      result_ = std::unexpected(error.error());
    } else if (*error == 0) {
      result_ = MakeStream();
    } else {
      DetachChannel();
      result_ = std::unexpected(base::make_errno(*error));
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
  InetAddress peer_;
  int fd_{-1};
  std::optional<Channel> channel_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_{};
  std::optional<base::Result<ReactorStream>> result_;
};

class SleepAwaiter {
public:
  SleepAwaiter(EventLoop* loop, std::chrono::milliseconds delay) noexcept
      : loop_(loop), delay_(delay) {}

  bool await_ready() const noexcept { return delay_.count() <= 0; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    VEXO_DCHECK(loop_->IsInLoopThread(), "SleepAwaiter: wrong EventLoop thread");
    scheduler_ = &coro::Scheduler::RequireCurrent();
    resume_work_.handle = continuation;
    const auto seconds = std::chrono::duration<double>(delay_).count();
    loop_->RunAfter(seconds, [this] { scheduler_->Schedule(&resume_work_); });
    return true;
  }

  void await_resume() const noexcept {}

private:
  EventLoop* loop_;
  std::chrono::milliseconds delay_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_{};
};

}  // namespace

ReactorConnector::ReactorConnector(EventLoop* loop) noexcept : loop_(loop) {}

ReactorConnector::ReactorConnector(ReactorConnector&& other) noexcept
    : loop_(std::exchange(other.loop_, nullptr)) {}

ReactorConnector& ReactorConnector::operator=(ReactorConnector&& other) noexcept {
  if (this != &other) {
    loop_ = std::exchange(other.loop_, nullptr);
  }
  return *this;
}

coro::Task<base::Result<ReactorStream>> ReactorConnector::Connect(std::string_view host,
                                                                  std::uint16_t port) {
  auto address = ParseIPv4Address(host, port);
  if (!address.has_value()) {
    co_return std::unexpected(address.error());
  }
  co_return co_await ConnectAwaiter(loop_, *address);
}

coro::Task<void> ReactorConnector::SleepFor(std::chrono::milliseconds delay) {
  co_await SleepAwaiter(loop_, delay);
}

}  // namespace vexo::net
