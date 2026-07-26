// Copyright (c) 2026 Arsenova
#include "coropact/luring/listener.h"

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

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/luring/detail/close_state.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/op.h"
#include "coropact/luring/stream.h"
#include "coropact/net/endpoint.h"

namespace coropact::luring {

namespace {

using AcceptResult = base::Result<LUringStream>;

base::Result<int> CreatedListenFd(const net::Endpoint& listen_addr,
                                  const LUringListenOptions& options) noexcept {
  const int fd = ::socket(listen_addr.native_family(),
                          SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
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

  if (::bind(fd, listen_addr.sock_addr(), listen_addr.sock_addr_len()) < 0) {
    return fail(base::CurrentErrno());
  }

  if (::listen(fd, options.backlog) < 0) {
    return fail(base::CurrentErrno());
  }

  return fd;
}

base::Result<net::Endpoint> GetLocalAddress(int fd) noexcept {
  sockaddr_storage addr{};
  auto len = static_cast<socklen_t>(sizeof(addr));
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    return std::unexpected(base::CurrentErrno());
  }
  return net::Endpoint(reinterpret_cast<const sockaddr*>(&addr), len);
}

AcceptResult MakeStream(LUringLoop* loop, int fd, const sockaddr_storage& peer_addr,
                        socklen_t peer_len) noexcept {
  return LUringStream(loop, fd,
                      net::Endpoint(reinterpret_cast<const sockaddr*>(&peer_addr), peer_len));
}

}  // namespace

class LUringListener::AcceptAwaiter
    : public detail::LUringOpHook<LUringListener::AcceptAwaiter> {
  friend void detail::DispatchAcceptComplete(LUringOp* op) noexcept;

public:
  using OpHook = detail::LUringOpHook<AcceptAwaiter>;

  explicit AcceptAwaiter(LUringListener& listener) noexcept
      : OpHook(LUringOpKind::kAcceptComplete), listener_(&listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (listener_->closed_ || listener_->fd_ < 0) {
      immediate_.emplace(std::unexpected(base::make_errno(EBADF)));
      return false;
    }
    ++listener_->pending_accepts_;
    op()->kind = LUringOpKind::kAcceptComplete;
    op()->resume_work.SetHandle(continuation);
    peer_len_ = static_cast<socklen_t>(sizeof(peer_addr_));

    auto submitted =
        listener_->loop_->SubmitOp(op(), [this, fd = listener_->fd_](io_uring_sqe* sqe) noexcept {
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
    auto* self = static_cast<OpHook*>(op)->owner();
    if (self->listener_ != nullptr) {
      LUringListener* listener = self->listener_;
      assert(listener->pending_accepts_ > 0);
      --listener->pending_accepts_;

      if (!op->result.has_value()) {
        self->immediate_ = std::unexpected(op->result.error());
      } else if (*op->result < 0) {
        self->immediate_ = std::unexpected(base::make_neg_errno(*op->result));
      } else {
        self->immediate_ =
            MakeStream(listener->loop_, *op->result, self->peer_addr_, self->peer_len_);
      }

      self->listener_ = nullptr;
      listener->NotifyCloseProgress();
    }
  }

  LUringOp* op() noexcept { return static_cast<OpHook*>(this); }

  LUringListener* listener_;
  sockaddr_storage peer_addr_{};
  socklen_t peer_len_{sizeof(peer_addr_)};
  std::optional<AcceptResult> immediate_;
};

class LUringListener::CloseAwaiter
    : public detail::LUringOpHook<LUringListener::CloseAwaiter> {
  friend void detail::DispatchListenerCloseComplete(LUringOp* op) noexcept;

public:
  using OpHook = detail::LUringOpHook<CloseAwaiter>;

  explicit CloseAwaiter(LUringListener& listener) noexcept
      : OpHook(LUringOpKind::kListenerCloseComplete), listener_(&listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (listener_->pending_close_ != nullptr) {
      state_.SetError(base::make_errno(EBUSY));
      return false;
    }
    if (listener_->closed_ || listener_->fd_ < 0) {
      state_.SetSuccess();
      return false;
    }

    listener_->closed_ = true;
    if (listener_->pending_accepts_ == 0) {
      state_.SetResult(CloseFd());
      return false;
    }

    listener_->pending_close_ = this;
    continuation_ = continuation;
    op()->kind = LUringOpKind::kListenerCloseComplete;

    auto submitted =
        listener_->loop_->SubmitOp(op(), [fd = listener_->fd_](io_uring_sqe* sqe) noexcept {
          io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
        });
    if (!submitted.has_value()) {
      listener_->pending_close_ = nullptr;
      listener_->closed_ = false;
      state_.SetError(submitted.error());
      return false;
    }

    return true;
  }

  base::Result<void> await_resume() noexcept {
    assert(state_.HasResult());
    return state_.TakeResult();
  }

  void TryComplete(LUringOp* current = nullptr) noexcept {
    if (state_.Completed() || listener_ == nullptr || !state_.CancelCompleted()) {
      return;
    }
    if (listener_->pending_accepts_ != 0) {
      return;
    }

    state_.MarkCompleted();
    LUringLoop* loop = listener_->loop_;
    listener_->pending_close_ = nullptr;
    state_.SetResult(CloseFd());
    listener_ = nullptr;
    op()->resume_work.SetHandle(continuation_);
    if (current != op()) {
      loop->ScheduleCompletion(&op()->resume_work);
    }
  }

private:
  static void OnCancelComplete(LUringOp* op) noexcept {
    auto* self = static_cast<OpHook*>(op)->owner();
    self->state_.MarkCancelCompleted();
    self->TryComplete(op);
  }

  LUringOp* op() noexcept { return static_cast<OpHook*>(this); }

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
  std::coroutine_handle<> continuation_{};
  detail::LUringCloseState state_;
};

namespace detail {

void DispatchAcceptComplete(LUringOp* op) noexcept {
  LUringListener::AcceptAwaiter::OnComplete(op);
}

void DispatchListenerCloseComplete(LUringOp* op) noexcept {
  LUringListener::CloseAwaiter::OnCancelComplete(op);
}

}  // namespace detail

base::Result<LUringListener> LUringListener::Create(LUringLoop* loop,
                                                    const net::Endpoint& listen_addr,
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
  COROPACT_CHECK(loop_ == nullptr || loop_ == other_loop,
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

base::Result<net::Endpoint> LUringListener::LocalAddress() const noexcept {
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
  COROPACT_CHECK(loop_ != nullptr, "LUringListener move destination is not initialized");
  COROPACT_CHECK(loop_->IsInLoopThread(), "LUringListener move called from wrong LUringLoop thread");
  COROPACT_CHECK(pending_accepts_ == 0,
             "LUringListener move destination has pending accept operations");
  COROPACT_CHECK(pending_close_ == nullptr,
             "LUringListener move destination has a pending close operation");

  const int fd = std::exchange(fd_, -1);
  if (fd >= 0) {
    ::close(fd);
  }
}

LUringLoop* LUringListener::PrepareMove(LUringListener& other) noexcept {
  COROPACT_CHECK(other.loop_ != nullptr, "LUringListener move source is not initialized");
  COROPACT_CHECK(other.loop_->IsInLoopThread(),
             "LUringListener move called from wrong LUringLoop thread");
  COROPACT_CHECK(other.pending_accepts_ == 0,
             "LUringListener cannot move with pending accept operations");
  COROPACT_CHECK(other.pending_close_ == nullptr,
             "LUringListener cannot move with a pending close operation");

  LUringLoop* loop = other.loop_;
  other.loop_ = nullptr;
  return loop;
}

}  // namespace coropact::luring
