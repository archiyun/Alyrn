// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/reactor_listener.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <coroutine>
#include <cstdlib>
#include <expected>
#include <memory>
#include <new>
#include <optional>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/work.h"
#include "vexo/log/logger.h"
#include "vexo/net/net_utils.h"

namespace vexo::net {
namespace {

using AcceptedStream = std::unique_ptr<ReactorStream>;
using AcceptResult = base::Result<AcceptedStream>;

bool IsWouldBlock(int err) noexcept { return err == EAGAIN || err == EWOULDBLOCK; }

base::Error SocketError(int fd) noexcept {
  int err = 0;
  socklen_t len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    return base::make_errno(errno);
  }
  if (err == 0) {
    err = EIO;
  }
  return base::make_errno(err);
}

int CreateListenSocket() {
  auto fd = CreateNonBlockingSocket();
  if (!fd.has_value()) {
    LOG_FATAL() << "failed to create reactor listener socket: error=" << fd.error().value()
                << " message=" << fd.error().message();
    std::abort();
  }
  return *fd;
}

}  // namespace

class ReactorListener::AcceptAwaiter {
public:
  explicit AcceptAwaiter(ReactorListener& listener) noexcept : listener_(&listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    assert(listener_->loop_->IsInLoopThread());
    assert(listener_->pending_accept_ == nullptr &&
           "only one pending accept is supported per listener");

    scheduler_ = &coro::Scheduler::RequireCurrent();
    resume_work_.handle = continuation;

    AcceptResult result = TryAccept();
    if (result.has_value() || !IsWouldBlock(result.error().value())) {
      result_ = std::move(result);
      return false;
    }

    listener_->pending_accept_ = this;
    if (!listener_->channel_.IsReading()) {
      listener_->channel_.EnableReading();
    }
    return true;
  }

  AcceptResult await_resume() noexcept {
    assert(result_.has_value());
    return std::move(*result_);
  }

  void Complete(AcceptResult result) noexcept {
    result_ = std::move(result);
    assert(scheduler_ != nullptr);
    scheduler_->Schedule(&resume_work_);
  }

  void OnReady() noexcept {
    AcceptResult result = TryAccept();
    if (!result.has_value() && IsWouldBlock(result.error().value())) {
      return;
    }
    listener_->CompleteAccept(std::move(result));
  }

private:
  AcceptResult TryAccept() noexcept {
    int fd = -1;
    InetAddress peer_addr(0);
    do {
      fd = listener_->socket_.Accept(&peer_addr);
    } while (fd < 0 && errno == EINTR);

    if (fd < 0) {
      return std::unexpected(base::make_errno(errno));
    }
    AcceptedStream stream(new (std::nothrow) ReactorStream(listener_->loop_, fd, peer_addr));
    if (!stream) {
      ::close(fd);
      return std::unexpected(base::make_errno(ENOMEM));
    }
    return stream;
  }

  ReactorListener* listener_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_{};
  std::optional<AcceptResult> result_;
};

ReactorListener::ReactorListener(EventLoop* loop, const InetAddress& listen_addr)
    : loop_(loop), socket_(CreateListenSocket()), channel_(loop, socket_.fd()) {
  assert(loop_ != nullptr);

  socket_.set_reuse_addr(true);
  socket_.BindAddress(listen_addr);
  socket_.Listen();

  channel_.set_read_callback([this](vexo::time::Timestamp ts) { HandleRead(ts); });
  channel_.set_error_callback([this] { HandleError(); });
}

ReactorListener::~ReactorListener() {
  assert(loop_->IsInLoopThread());
  assert(pending_accept_ == nullptr);
  DetachChannel();
}

coro::Task<base::Result<std::unique_ptr<ReactorStream>>> ReactorListener::Accept() {
  if (closed_) {
    co_return std::unexpected(base::make_errno(EBADF));
  }
  co_return co_await AcceptAwaiter(*this);
}

coro::Task<base::Result<void>> ReactorListener::Close() {
  if (closed_) {
    co_return base::Result<void>{};
  }

  closed_ = true;
  if (pending_accept_ != nullptr) {
    CompleteAccept(std::unexpected(base::make_errno(ECANCELED)));
  }
  DetachChannel();
  socket_.Close();
  co_return base::Result<void>{};
}

base::Result<InetAddress> ReactorListener::LocalAddress() const {
  if (closed_) {
    return std::unexpected(base::make_errno(EBADF));
  }
  return get_local_addr(socket_.fd());
}

void ReactorListener::HandleRead(vexo::time::Timestamp /*receive_time*/) {
  assert(loop_->IsInLoopThread());
  if (pending_accept_ != nullptr) {
    pending_accept_->OnReady();
  }
}

void ReactorListener::HandleError() {
  assert(loop_->IsInLoopThread());
  CompleteAccept(std::unexpected(SocketError(socket_.fd())));
}

void ReactorListener::CompleteAccept(base::Result<std::unique_ptr<ReactorStream>> result) {
  assert(loop_->IsInLoopThread());
  AcceptAwaiter* awaiter = std::exchange(pending_accept_, nullptr);
  if (awaiter == nullptr) {
    return;
  }
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  awaiter->Complete(std::move(result));
}

void ReactorListener::DetachChannel() {
  assert(loop_->IsInLoopThread());
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (loop_->HasChannel(&channel_)) {
    channel_.Remove();
  }
}

}  // namespace vexo::net
