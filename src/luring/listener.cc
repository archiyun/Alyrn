// Copyright (c) 2026 Arsenova
#include "vexo/luring/listener.h"

#include <liburing.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <coroutine>
#include <expected>
#include <optional>
#include <utility>

#include "vexo/base/check.h"
#include "vexo/base/error.h"
#include "vexo/luring/loop.h"
#include "vexo/luring/op.h"
#include "vexo/luring/stream.h"
#include "vexo/net/inet_address.h"

namespace vexo::luring {

namespace {

using AcceptResult = base::Result<LUringStream>;

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
  return LUringStream(loop, fd, net::InetAddress(peer_addr));
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
    ++listener_->pending_accepts_;
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
      --listener_->pending_accepts_;
      immediate_.emplace(std::unexpected(submitted.error()));
      return false;
    }
    return true;
  }

  AcceptResult await_resume() noexcept {
    assert(immediate_.has_value());
    return std::move(*immediate_);
  }

private:
  static void OnComplete(LUringOp* op) noexcept {
    auto* self = static_cast<AcceptAwaiter*>(op->owner);
    if (self->listener_ != nullptr) {
      LUringListener* listener = self->listener_;
      assert(listener->pending_accepts_ > 0);
      --listener->pending_accepts_;

      if (!op->result.has_value()) {
        self->immediate_ = std::unexpected(op->result.error());
      } else if (*op->result < 0) {
        self->immediate_ = std::unexpected(base::make_neg_errno(*op->result));
      } else {
        self->immediate_ = MakeStream(listener->loop_, *op->result, self->peer_addr_);
      }

      self->listener_ = nullptr;
      listener->NotifyCloseProgress();
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
    if (listener_->pending_accepts_ == 0) {
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
    if (listener_->pending_accepts_ != 0) {
      return;
    }

    completed_ = true;
    LUringLoop* loop = listener_->loop_;
    listener_->pending_close_ = nullptr;
    result_ = CloseFd();
    listener_ = nullptr;
    loop->ScheduleCompletion(&resume_work_);
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

base::Result<LUringListener> LUringListener::Create(LUringLoop* loop,
                                                    const net::InetAddress& listen_addr,
                                                    LUringListenOptions options) noexcept {
  assert(loop != nullptr);
  assert(loop->IsInLoopThread());

  auto fd = CreatedListenFd(listen_addr, options);
  if (!fd.has_value()) {
    return std::unexpected(fd.error());
  }

  return LUringListener(loop, *fd);
}

LUringListener::LUringListener(LUringLoop* loop, int fd) noexcept : loop_(loop), fd_(fd) {
  assert(loop_ != nullptr);
  assert(fd_ >= 0);
}

LUringListener::LUringListener(LUringListener&& other) noexcept
    : loop_(PrepareMove(other)),
      fd_(std::exchange(other.fd_, -1)),
      pending_accepts_(0),
      pending_close_(nullptr),
      closed_(other.closed_) {
  other.closed_ = true;
}

LUringListener& LUringListener::operator=(LUringListener&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  LUringLoop* other_loop = PrepareMove(other);
  VEXO_CHECK(loop_ == nullptr || loop_ == other_loop,
             "LUringListener move requires both objects to use the same LUringLoop");
  if (loop_ != nullptr) {
    ResetForMove();
  }

  loop_ = other_loop;
  fd_ = std::exchange(other.fd_, -1);
  pending_accepts_ = 0;
  pending_close_ = nullptr;
  closed_ = other.closed_;
  other.closed_ = true;
  return *this;
}

LUringListener::~LUringListener() {
  assert(pending_accepts_ == 0);
  assert(pending_close_ == nullptr);
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

coro::Task<base::Result<LUringStream>> LUringListener::Accept() {
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

void LUringListener::ResetForMove() noexcept {
  VEXO_CHECK(loop_ != nullptr, "LUringListener move destination is not initialized");
  VEXO_CHECK(loop_->IsInLoopThread(), "LUringListener move called from wrong LUringLoop thread");
  VEXO_CHECK(pending_accepts_ == 0,
             "LUringListener move destination has pending accept operations");
  VEXO_CHECK(pending_close_ == nullptr,
             "LUringListener move destination has a pending close operation");

  const int fd = std::exchange(fd_, -1);
  if (fd >= 0) {
    ::close(fd);
  }
}

LUringLoop* LUringListener::PrepareMove(LUringListener& other) noexcept {
  VEXO_CHECK(other.loop_ != nullptr, "LUringListener move source is not initialized");
  VEXO_CHECK(other.loop_->IsInLoopThread(),
             "LUringListener move called from wrong LUringLoop thread");
  VEXO_CHECK(other.pending_accepts_ == 0,
             "LUringListener cannot move with pending accept operations");
  VEXO_CHECK(other.pending_close_ == nullptr,
             "LUringListener cannot move with a pending close operation");

  LUringLoop* loop = other.loop_;
  other.loop_ = nullptr;
  return loop;
}

}  // namespace vexo::luring
