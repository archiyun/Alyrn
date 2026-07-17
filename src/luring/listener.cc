#include "vexo/luring/listener.h"

#include <liburing.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <coroutine>
#include <expected>
#include <memory>
#include <new>
#include <optional>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/luring/loop.h"
#include "vexo/luring/op.h"
#include "vexo/luring/stream.h"
#include "vexo/net/inet_address.h"

namespace vexo::luring {

namespace {

using AcceptedStream = std::unique_ptr<LUringStream>;
using AcceptResult = base::Result<AcceptedStream>;

base::Result<int> CreatedListenFd(const net::InetAddress& listen_addr,
                                  const LUringListenOptions& options) noexcept {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(base::CurrentErrno());
  }

  auto fail = [fd](base::Error error) -> base::Result<int> {
    ::close(fd);
    return std::unexpected(error);
  };

  int on = 1;

  if (options.reuse_addr) {
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
      return fail(base::CurrentErrno());
    }
  }

  if (options.reuse_port) {
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on)) < 0) {
      return fail(base::CurrentErrno());
    }
  }

  const sockaddr_in addr = listen_addr.sock_addr();
  if (::bind(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
    return fail(base::CurrentErrno());
  }

  if (::listen(fd, options.backlog) < 0) {
    return fail(base::CurrentErrno());
  }

  return fd;
}

base::Result<net::InetAddress> GetLocalAddress(int fd) noexcept {
  sockaddr_in addr{};
  auto len = static_cast<socklen_t>(sizeof(addr));
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    return std::unexpected(base::CurrentErrno());
  }
  return net::InetAddress(addr);
}

AcceptResult MakeStream(LUringLoop* loop, int fd, const sockaddr_in& peer_addr) noexcept {
  auto* stream = new (std::nothrow) LUringStream(loop, fd, net::InetAddress(peer_addr));
  if (stream == nullptr) {
    ::close(fd);
    return std::unexpected(base::make_errno(ENOMEM));
  }
  return AcceptedStream(stream);
}

}  // namespace

class LUringListener::AcceptAwaiter {
public:
  explicit AcceptAwaiter(LUringListener& listener) noexcept : listener_(&listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (listener_->closed_ || listener_->fd_ < 0) {
      immediate_.emplace(std::unexpected(base::make_errno(EBADF)));
      return false;
    }
    if (listener_->pending_accept_ != nullptr) {
      immediate_.emplace(std::unexpected(base::make_errno(EBUSY)));
      return false;
    }

    listener_->pending_accept_ = this;
    op_.kind = LUringOpKind::kAccept;
    op_.continuation_ = continuation;
    op_.resume_work.handle = continuation;
    op_.owner = this;
    op_.on_complete = &AcceptAwaiter::OnComplete;
    peer_len_ = static_cast<socklen_t>(sizeof(peer_addr_));

    auto submitted =
        listener_->loop_->SubmitOp(&op_, [this, fd = listener_->fd_](io_uring_sqe* sqe) noexcept {
          io_uring_prep_accept(sqe, fd, reinterpret_cast<sockaddr*>(&peer_addr_), &peer_len_,
                               SOCK_NONBLOCK | SOCK_CLOEXEC);
        });

    if (!submitted.has_value()) {
      listener_->pending_accept_ = nullptr;
      immediate_.emplace(std::unexpected(submitted.error()));
      return false;
    }
    return true;
  }

  AcceptResult await_resume() noexcept {
    if (immediate_.has_value()) {
      return std::move(*immediate_);
    }
    assert(op_.completed);

    if (!op_.result.has_value()) {
      return std::unexpected(op_.result.error());
    }

    if (*op_.result < 0) {
      return std::unexpected(base::make_neg_errno(*op_.result));
    }

    return MakeStream(listener_->loop_, *op_.result, peer_addr_);
  }

private:
  static void OnComplete(LUringOp* op) noexcept {
    auto* self = static_cast<AcceptAwaiter*>(op->owner);
    if (self->listener_ != nullptr && self->listener_->pending_accept_ == self) {
      self->listener_->pending_accept_ = nullptr;
      self->listener_->NotifyCloseProgress();
    }
  }

  LUringListener* listener_;
  LUringOp op_{.kind = LUringOpKind::kAccept};
  sockaddr_in peer_addr_{};
  socklen_t peer_len_{sizeof(peer_addr_)};
  std::optional<AcceptResult> immediate_;
};

class LUringListener::CloseAwaiter {
public:
  explicit CloseAwaiter(LUringListener& listener) noexcept : listener_(&listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (listener_->pending_close_ != nullptr) {
      result_ = std::unexpected(base::make_errno(EBUSY));
      return false;
    }
    if (listener_->closed_ || listener_->fd_ < 0) {
      result_ = base::Result<void>{};
      return false;
    }

    listener_->closed_ = true;
    if (listener_->pending_accept_ == nullptr) {
      result_ = CloseFd();
      return false;
    }

    listener_->pending_close_ = this;
    resume_work_.handle = continuation;
    cancel_op_.kind = LUringOpKind::kClose;
    cancel_op_.owner = this;
    cancel_op_.on_complete = &CloseAwaiter::OnCancelComplete;

    auto submitted =
        listener_->loop_->SubmitOp(&cancel_op_, [fd = listener_->fd_](io_uring_sqe* sqe) noexcept {
          io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
        });
    if (!submitted.has_value()) {
      listener_->pending_close_ = nullptr;
      listener_->closed_ = false;
      result_ = std::unexpected(submitted.error());
      return false;
    }

    return true;
  }

  base::Result<void> await_resume() noexcept {
    assert(result_.has_value());
    return std::move(*result_);
  }

  void TryComplete() noexcept {
    if (completed_ || listener_ == nullptr || !cancel_completed_) {
      return;
    }
    if (listener_->pending_accept_ != nullptr) {
      return;
    }

    completed_ = true;
    LUringLoop* loop = listener_->loop_;
    listener_->pending_close_ = nullptr;
    result_ = CloseFd();
    listener_ = nullptr;
    loop->Schedule(&resume_work_);
  }

private:
  static void OnCancelComplete(LUringOp* op) noexcept {
    auto* self = static_cast<CloseAwaiter*>(op->owner);
    self->cancel_completed_ = true;
    self->TryComplete();
  }

  base::Result<void> CloseFd() noexcept {
    const int fd = std::exchange(listener_->fd_, -1);
    if (fd < 0) {
      return base::Result<void>{};
    }
    if (::close(fd) < 0) {
      return std::unexpected(base::CurrentErrno());
    }
    return base::Result<void>{};
  }

  LUringListener* listener_;
  LUringOp cancel_op_{.kind = LUringOpKind::kClose};
  coro::ResumeWork resume_work_{};
  std::optional<base::Result<void>> result_;
  bool cancel_completed_{false};
  bool completed_{false};
};

base::Result<std::unique_ptr<LUringListener>> LUringListener::Create(
    LUringLoop* loop, const net::InetAddress& listen_addr, LUringListenOptions options) noexcept {
  assert(loop != nullptr);
  assert(loop->IsInLoopThread());

  auto fd = CreatedListenFd(listen_addr, options);
  if (!fd.has_value()) {
    return std::unexpected(fd.error());
  }

  auto* listener = new (std::nothrow) LUringListener(loop, *fd);
  if (listener == nullptr) {
    ::close(*fd);
    return std::unexpected(base::make_errno(ENOMEM));
  }

  return std::unique_ptr<LUringListener>(listener);
}

LUringListener::LUringListener(LUringLoop* loop, int fd) noexcept : loop_(loop), fd_(fd) {
  assert(loop_ != nullptr);
  assert(fd_ >= 0);
}

LUringListener::~LUringListener() {
  assert(pending_accept_ == nullptr);
  assert(pending_close_ == nullptr);
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

coro::Task<base::Result<std::unique_ptr<LUringStream>>> LUringListener::Accept() {
  co_return co_await AcceptAwaiter(*this);
}

coro::Task<base::Result<void>> LUringListener::Close() { co_return co_await CloseAwaiter(*this); }

base::Result<net::InetAddress> LUringListener::LocalAddress() const noexcept {
  if (closed_ || fd_ < 0) {
    return std::unexpected(base::make_errno(EBADF));
  }
  return GetLocalAddress(fd_);
}

void LUringListener::NotifyCloseProgress() noexcept {
  if (pending_close_ != nullptr) {
    pending_close_->TryComplete();
  }
}

}  // namespace vexo::luring
