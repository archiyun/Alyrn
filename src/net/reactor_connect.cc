// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/reactor_connect.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <chrono>
#include <coroutine>
#include <expected>
#include <memory>
#include <new>
#include <optional>
#include <utility>

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
    return std::unexpected(base::make_errno(errno));
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
    assert(loop_->IsInLoopThread());
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
      result_ = std::unexpected(base::make_errno(errno));
      return false;
    }

    channel_ = std::make_unique<Channel>(loop_, fd_);
    channel_->set_write_callback([this] { OnReady(); });
    channel_->set_error_callback([this] { OnReady(); });
    channel_->EnableWriting();
    return true;
  }

  base::Result<std::unique_ptr<ReactorStream>> await_resume() noexcept {
    assert(result_.has_value());
    return std::move(*result_);
  }

private:
  base::Result<std::unique_ptr<ReactorStream>> MakeStream() noexcept {
    DetachChannel();
    auto stream =
        std::unique_ptr<ReactorStream>(new (std::nothrow) ReactorStream(loop_, fd_, peer_));
    if (!stream) {
      return std::unexpected(base::make_errno(ENOMEM));
    }
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
    if (loop_->HasChannel(channel_.get())) {
      channel_->Remove();
    }
    channel_.reset();
  }

  EventLoop* loop_;
  InetAddress peer_;
  int fd_{-1};
  std::unique_ptr<Channel> channel_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_{};
  std::optional<base::Result<std::unique_ptr<ReactorStream>>> result_;
};

class SleepAwaiter {
public:
  SleepAwaiter(EventLoop* loop, std::chrono::milliseconds delay) noexcept
      : loop_(loop), delay_(delay) {}

  bool await_ready() const noexcept { return delay_.count() <= 0; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    assert(loop_->IsInLoopThread());
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

coro::Task<base::Result<std::unique_ptr<ReactorStream>>> ReactorConnector::Connect(
    std::string_view host, std::uint16_t port) {
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
