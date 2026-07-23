// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/net/reactor_listener.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <expected>
#include <optional>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/work.h"
#include "coropact/log/logger.h"
#include "coropact/net/net_utils.h"

namespace coropact::net {
namespace {

bool IsWouldBlock(int err) noexcept { return err == EAGAIN || err == EWOULDBLOCK; }

base::Error SocketError(int fd) noexcept {
  int err = 0;
  auto len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    return base::CurrentErrno();
  }
  if (err == 0) {
    err = EIO;
  }
  return base::make_errno(err);
}

EventLoop* CheckLoop(EventLoop* loop) noexcept {
  COROPACT_CHECK(loop != nullptr, "ReactorListener: loop must not be null");
  return loop;
}

base::Result<Socket> TryCreateListenSocket(const InetAddress& listen_addr,
                                           ReactorListenerOptions options) noexcept {
  auto fd = CreateNonBlockingSocket();
  if (!fd.has_value()) {
    return std::unexpected(fd.error());
  }

  Socket socket(*fd);

  auto reuse_addr = set_reuse_addr(socket.fd(), options.reuse_addr);
  if (!reuse_addr.has_value()) {
    return std::unexpected(reuse_addr.error());
  }

  if (options.reuse_port) {
    auto reuse_port = set_reuse_port(socket.fd(), true);
    if (!reuse_port.has_value()) {
      return std::unexpected(reuse_port.error());
    }
  }

  if (::bind(socket.fd(), reinterpret_cast<const sockaddr*>(&listen_addr.sock_addr()),
             static_cast<socklen_t>(sizeof(sockaddr_in))) < 0) {
    return std::unexpected(base::CurrentErrno());
  }

  if (::listen(socket.fd(), SOMAXCONN) < 0) {
    return std::unexpected(base::CurrentErrno());
  }

  return socket;
}

int CreateListenSocket() {
  auto fd = CreateNonBlockingSocket();
  if (!fd.has_value()) {
    LOG_FATAL() << "failed to create reactor listener socket: error=" << fd.error().value()
                << " message=" << fd.error().message();
    COROPACT_CHECK(false, "ReactorListener: failed to create listening socket");
  }
  return *fd;
}

}  // namespace

class ReactorListener::AcceptAwaiter {
public:
  explicit AcceptAwaiter(ReactorListener& listener) noexcept : listener_(&listener) {}

  [[nodiscard]] bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    COROPACT_DCHECK(listener_->loop_->IsInLoopThread(), "AcceptAwaiter: wrong EventLoop thread");
    COROPACT_DCHECK(listener_->pending_accept_ == nullptr,
                "AcceptAwaiter: only one pending accept is supported per listener");

    scheduler_ = &coro::Scheduler::RequireCurrent();
    resume_work_.handle = continuation;

    base::Result<ReactorStream> result = TryAccept();
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

  base::Result<ReactorStream> await_resume() noexcept {
    COROPACT_DCHECK(result_.has_value(), "AcceptAwaiter: result is not ready");
    return std::move(*result_);
  }

  void Complete(base::Result<ReactorStream> result) noexcept {
    result_ = std::move(result);
    COROPACT_DCHECK(scheduler_ != nullptr, "AcceptAwaiter: scheduler is not bound");
    scheduler_->Schedule(&resume_work_);
  }

  void OnReady() noexcept {
    base::Result<ReactorStream> result = TryAccept();
    if (!result.has_value() && IsWouldBlock(result.error().value())) {
      return;
    }
    listener_->CompleteAccept(std::move(result));
  }

private:
  base::Result<ReactorStream> TryAccept() noexcept {
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
  coro::ResumeWork resume_work_;
  std::optional<base::Result<ReactorStream>> result_;
};

ReactorListener::ReactorListener(EventLoop* loop, const InetAddress& listen_addr,
                                 ReactorListenerOptions options)
    : loop_(CheckLoop(loop)), socket_(CreateListenSocket()), channel_(loop_, socket_.fd()) {
  socket_.set_reuse_addr(options.reuse_addr);
  if (options.reuse_port) {
    socket_.set_reuse_port(true);
  }
  socket_.BindAddress(listen_addr);
  socket_.Listen();

  BindChannelCallbacks();
}

ReactorListener::ReactorListener(EventLoop* loop, Socket socket) noexcept
    : loop_(CheckLoop(loop)), socket_(std::move(socket)), channel_(loop_, socket_.fd()) {
  BindChannelCallbacks();
}

base::Result<ReactorListener> ReactorListener::Create(EventLoop* loop,
                                                      const InetAddress& listen_addr,
                                                      ReactorListenerOptions options) noexcept {
  if (loop == nullptr) {
    return std::unexpected(base::make_errno(EINVAL));
  }

  auto socket = TryCreateListenSocket(listen_addr, options);
  if (!socket.has_value()) {
    return std::unexpected(socket.error());
  }
  return ReactorListener(loop, std::move(*socket));
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
  COROPACT_CHECK(loop_ == nullptr || loop_ == other_loop,
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
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorListener destructor called from wrong thread");
  COROPACT_DCHECK(pending_accept_ == nullptr, "ReactorListener destroyed with a pending accept");
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

void ReactorListener::HandleRead(coropact::time::Timestamp /*receive_time*/) {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorListener::HandleRead called from wrong thread");
  if (pending_accept_ != nullptr) {
    pending_accept_->OnReady();
  }
}

void ReactorListener::HandleError() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorListener::HandleError called from wrong thread");
  CompleteAccept(std::unexpected(SocketError(socket_.fd())));
}

void ReactorListener::CompleteAccept(base::Result<ReactorStream> result) {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorListener::CompleteAccept called from wrong thread");
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
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorListener::DetachChannel called from wrong thread");
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (loop_->HasChannel(&channel_)) {
    channel_.Remove();
  }
}

void ReactorListener::BindChannelCallbacks() noexcept {
  try {
    channel_.set_read_callback([this](coropact::time::Timestamp ts) { HandleRead(ts); });
    channel_.set_error_callback([this] { HandleError(); });
  } catch (...) {
    COROPACT_CHECK(false, "ReactorListener: failed to bind channel callbacks");
  }
}

void ReactorListener::ResetForMove() noexcept {
  COROPACT_CHECK(loop_ != nullptr, "ReactorListener move destination is not initialized");
  COROPACT_CHECK(loop_->IsInLoopThread(), "ReactorListener move called from wrong EventLoop thread");
  COROPACT_CHECK(pending_accept_ == nullptr, "ReactorListener move destination has a pending accept");
  DetachChannel();
  socket_.Close();
}

EventLoop* ReactorListener::PrepareMove(ReactorListener& other) noexcept {
  COROPACT_CHECK(other.loop_ != nullptr, "ReactorListener move source is not initialized");
  COROPACT_CHECK(other.loop_->IsInLoopThread(),
             "ReactorListener move called from wrong EventLoop thread");
  COROPACT_CHECK(other.pending_accept_ == nullptr,
             "ReactorListener cannot move with a pending accept operation");

  other.DetachChannel();
  EventLoop* loop = other.loop_;
  other.loop_ = nullptr;
  return loop;
}

}  // namespace coropact::net
