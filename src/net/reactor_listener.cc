// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/reactor_listener.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <expected>
#include <optional>
#include <utility>

#include "vexo/base/check.h"
#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/work.h"
#include "vexo/log/logger.h"
#include "vexo/net/net_utils.h"

namespace vexo::net {
namespace {

using AcceptResult = base::Result<ReactorStream>;

bool IsWouldBlock(int err) noexcept { return err == EAGAIN || err == EWOULDBLOCK; }

base::Error SocketError(int fd) noexcept {
  int err = 0;
  socklen_t len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    return base::CurrentErrno();
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
    VEXO_CHECK(false, "ReactorListener: failed to create listening socket");
  }
  return *fd;
}

}  // namespace

class ReactorListener::AcceptAwaiter {
public:
  explicit AcceptAwaiter(ReactorListener& listener) noexcept : listener_(&listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    VEXO_DCHECK(listener_->loop_->IsInLoopThread(), "AcceptAwaiter: wrong EventLoop thread");
    VEXO_DCHECK(listener_->pending_accept_ == nullptr,
                "AcceptAwaiter: only one pending accept is supported per listener");

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
    VEXO_DCHECK(result_.has_value(), "AcceptAwaiter: result is not ready");
    return std::move(*result_);
  }

  void Complete(AcceptResult result) noexcept {
    result_ = std::move(result);
    VEXO_DCHECK(scheduler_ != nullptr, "AcceptAwaiter: scheduler is not bound");
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
      return std::unexpected(base::CurrentErrno());
    }
    return ReactorStream(listener_->loop_, fd, peer_addr);
  }

  ReactorListener* listener_;
  coro::Scheduler* scheduler_{nullptr};
  coro::ResumeWork resume_work_{};
  std::optional<AcceptResult> result_;
};

ReactorListener::ReactorListener(EventLoop* loop, const InetAddress& listen_addr)
    : loop_(loop), socket_(CreateListenSocket()), channel_(loop, socket_.fd()) {
  VEXO_DCHECK(loop_ != nullptr, "ReactorListener: loop must not be null");

  socket_.set_reuse_addr(true);
  socket_.BindAddress(listen_addr);
  socket_.Listen();

  BindChannelCallbacks();
}

ReactorListener::ReactorListener(ReactorListener&& other) noexcept
    : loop_(PrepareMove(other)),
      socket_(std::move(other.socket_)),
      channel_(std::move(other.channel_)),
      pending_accept_(nullptr),
      closed_(other.closed_) {
  BindChannelCallbacks();
  other.closed_ = true;
}

ReactorListener& ReactorListener::operator=(ReactorListener&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  EventLoop* other_loop = PrepareMove(other);
  VEXO_CHECK(loop_ == nullptr || loop_ == other_loop,
             "ReactorListener move requires both objects to use the same EventLoop");
  if (loop_ != nullptr) {
    ResetForMove();
  }

  loop_ = other_loop;
  socket_ = std::move(other.socket_);
  channel_ = std::move(other.channel_);
  pending_accept_ = nullptr;
  closed_ = other.closed_;
  BindChannelCallbacks();
  other.closed_ = true;
  return *this;
}

ReactorListener::~ReactorListener() {
  if (loop_ == nullptr) {
    return;
  }
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorListener destructor called from wrong thread");
  VEXO_DCHECK(pending_accept_ == nullptr, "ReactorListener destroyed with a pending accept");
  DetachChannel();
}

coro::Task<base::Result<ReactorStream>> ReactorListener::Accept() {
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
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorListener::HandleRead called from wrong thread");
  if (pending_accept_ != nullptr) {
    pending_accept_->OnReady();
  }
}

void ReactorListener::HandleError() {
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorListener::HandleError called from wrong thread");
  CompleteAccept(std::unexpected(SocketError(socket_.fd())));
}

void ReactorListener::CompleteAccept(base::Result<ReactorStream> result) {
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorListener::CompleteAccept called from wrong thread");
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
  VEXO_DCHECK(loop_->IsInLoopThread(), "ReactorListener::DetachChannel called from wrong thread");
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (loop_->HasChannel(&channel_)) {
    channel_.Remove();
  }
}

void ReactorListener::BindChannelCallbacks() noexcept {
  try {
    channel_.set_read_callback([this](vexo::time::Timestamp ts) { HandleRead(ts); });
    channel_.set_error_callback([this] { HandleError(); });
  } catch (...) {
    VEXO_CHECK(false, "ReactorListener: failed to bind channel callbacks");
  }
}

void ReactorListener::ResetForMove() noexcept {
  VEXO_CHECK(loop_ != nullptr, "ReactorListener move destination is not initialized");
  VEXO_CHECK(loop_->IsInLoopThread(), "ReactorListener move called from wrong EventLoop thread");
  VEXO_CHECK(pending_accept_ == nullptr, "ReactorListener move destination has a pending accept");
  DetachChannel();
  socket_.Close();
}

EventLoop* ReactorListener::PrepareMove(ReactorListener& other) noexcept {
  VEXO_CHECK(other.loop_ != nullptr, "ReactorListener move source is not initialized");
  VEXO_CHECK(other.loop_->IsInLoopThread(),
             "ReactorListener move called from wrong EventLoop thread");
  VEXO_CHECK(other.pending_accept_ == nullptr,
             "ReactorListener cannot move with a pending accept operation");

  other.DetachChannel();
  EventLoop* loop = other.loop_;
  other.loop_ = nullptr;
  return loop;
}

}  // namespace vexo::net
