// SPDX-License-Identifier: MIT
#include "alyrn/uring/connector.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <cstdint>
#include <expected>
#include <string_view>
#include <utility>

#include "alyrn/backend/value_result_state.h"
#include "alyrn/detail/check.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"
#include "alyrn/uring/detail/completion_dispatch.h"
#include "alyrn/uring/detail/loop_access.h"
#include "alyrn/uring/detail/op.h"
#include "alyrn/uring/detail/operation_submission.h"
#include "alyrn/uring/detail/sqe_prep.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/stream.h"
#include "alyrn/uring/timer.h"

namespace alyrn::uring {

using namespace detail;

namespace {

Result<int> CreateSocket(sa_family_t family) noexcept {
  const int fd = ::socket(family, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(CurrentErrno());
  }
  return fd;
}

Result<void> SetNonBlocking(int fd) noexcept {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return std::unexpected(CurrentErrno());
  }
  if ((flags & O_NONBLOCK) != 0) {
    return {};
  }
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return std::unexpected(CurrentErrno());
  }
  return {};
}

struct ConnectCancelTag;

// --- ConnectAwaiter ---
// A connect timeout cancels the in-flight CONNECT with an async cancel
// request. Both CQEs name this awaiter, so it resumes only after both arrived.
class [[nodiscard]] ConnectAwaiter : public detail::OpHook<ConnectAwaiter>,
                                     public detail::OpHook<ConnectAwaiter, ConnectCancelTag> {
public:
  using ConnectHook = detail::OpHook<ConnectAwaiter>;
  using CancelHook = detail::OpHook<ConnectAwaiter, ConnectCancelTag>;

  ConnectAwaiter(Loop* loop, net::Endpoint peer, net::TcpOptions tcp_options,
                 time::Duration timeout) noexcept
      : ConnectHook(OpKind::kConnect),
        CancelHook(OpKind::kConnectCancelComplete),
        loop_(loop),
        peer_(peer),
        tcp_options_(tcp_options),
        timeout_(timeout) {}

  ~ConnectAwaiter() noexcept {
    ALYRN_CHECK(!ConnectOp()->resume_work.HasHandle() || ConnectOp()->CqeCompletionRecorded(),
                "ConnectAwaiter destroyed before its physical connect CQE settled");
    ALYRN_CHECK(!cancel_submitted_ || cancel_terminal_,
                "ConnectAwaiter destroyed before its timeout cancel CQE settled");
    CancelTimer();
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    ALYRN_CHECK(loop_ != nullptr, "Connector operation has no owner loop");
    ALYRN_CHECK(loop_->IsInLoopThread(), "Connector operation called from wrong Loop thread");
    if (Stopping()) {
      CompleteInline(std::unexpected(Errno(ECANCELED)));
      return false;
    }

    auto fd = CreateSocket(peer_.NativeFamily());
    if (!fd.HasValue()) {
      CompleteInline(std::unexpected(fd.Error()));
      return false;
    }
    fd_ = *fd;

    if (timeout_ > time::Duration::zero()) {
      auto timer = loop_->RunAfter(timeout_, [this] {
        // The queue already removed this timer before running it.
        timer_ = {};
        OnTimeout();
      });
      if (!timer.HasValue()) {
        CompleteInline(std::unexpected(timer.Error()));
        return false;
      }
      timer_ = *timer;
    }

    ConnectOp()->kind = OpKind::kConnect;
    return detail::SubmitAwaitingOperation(
        *loop_, *ConnectOp(), continuation,
        detail::PrepareConnect(fd_, peer_.SockAddr(), peer_.SockAddrLen()),
        [this](Error error) noexcept { CompleteInline(std::unexpected(error)); });
  }

  Result<Stream> await_resume() noexcept { return result_.Take(); }

  // CQE dispatch entry points.
  static void OnComplete(::alyrn::uring::detail::Op* op) noexcept {
    auto* self = ConnectHook::OwnerFrom(op);
    ALYRN_CHECK(op->result.HasValue(), "Uring Connect CQE is missing its result");
    self->connect_terminal_ = true;
    self->CancelTimer();

    if (self->timed_out_) {
      // Once the timeout fired the result is fixed, even if the connect won a
      // race with its cancellation; ReleasePhysicalRequest closes the socket.
      self->result_.SetError(Errno(ETIMEDOUT));
    } else if (*op->result < 0) {
      self->result_.SetError(NegErrno(*op->result));
    } else {
      auto nonblocking = SetNonBlocking(self->fd_);
      if (!nonblocking.HasValue()) {
        self->result_.SetError(nonblocking.Error());
      } else {
        self->result_.SetResult(self->MakeStream());
      }
    }

    ALYRN_CHECK(op->TryAuthorizeCoupledResult(), "Uring Connect result was authorized twice");
    ALYRN_CHECK(op->TryAuthorizeCoupledRelease(),
                "Uring Connect release was not authorized after its result");
    self->ReleasePhysicalRequest();

    if (self->cancel_submitted_ && !self->cancel_terminal_) {
      // The cancel CQE still names this awaiter: resume after it arrives.
      self->CancelOp()->resume_work.SetHandle(op->resume_work.Handle());
      op->resume_work.ClearHandle();
    }
  }

  static CompletionDisposition OnCancelComplete(::alyrn::uring::detail::Op* op) noexcept {
    auto* self = CancelHook::OwnerFrom(op);
    self->cancel_terminal_ = true;
    return CompletionDisposition{
        .kernel_request_terminal = true,
        .decrement_inflight = true,
        .resume_continuation = self->connect_terminal_ && op->resume_work.HasHandle(),
    };
  }

private:
  Op* ConnectOp() noexcept { return ConnectHook::Operation(); }
  const Op* ConnectOp() const noexcept { return ConnectHook::Operation(); }
  Op* CancelOp() noexcept { return CancelHook::Operation(); }

  bool Stopping() const noexcept {
    const backend::LoopState state = loop_->State();
    return state == backend::LoopState::kStopping || state == backend::LoopState::kStopped;
  }

  void OnTimeout() noexcept {
    if (connect_terminal_ || cancel_submitted_) {
      return;
    }
    timed_out_ = true;
    SubmitCancel();
  }

  void SubmitCancel() noexcept {
    CancelOp()->BeginNextRequest();
    auto submitted = LoopAccess::SubmitOp(
        *loop_, CancelOp(),
        PrepareCancelAllByUserData(reinterpret_cast<std::uint64_t>(ConnectOp())));
    if (submitted.HasValue()) {
      cancel_submitted_ = true;
      return;
    }
    if (Stopping()) {
      // The stop drain cancels every pending request, this connect included.
      return;
    }
    // The submission queue is full: retry on a later turn.
    auto retry = loop_->RunAfter(time::Milliseconds(1), [this] {
      timer_ = {};
      if (!connect_terminal_ && !cancel_submitted_) {
        SubmitCancel();
      }
    });
    if (retry.HasValue()) {
      timer_ = *retry;
      return;
    }
    // Neither a cancel request nor a timer can be submitted.
    loop_->RequestStop();
  }

  void CancelTimer() noexcept {
    if (timer_.Valid()) {
      (void)loop_->Cancel(std::exchange(timer_, {}));
    }
  }

  void CompleteInline(Result<Stream> result) noexcept {
    result_.SetResult(std::move(result));
    ALYRN_CHECK(ConnectOp()->TryAuthorizeCoupledResult(),
                "Uring Connect result was authorized twice");
    ALYRN_CHECK(ConnectOp()->TryAuthorizeCoupledRelease(),
                "Uring Connect release was not authorized after its result");
    ReleasePhysicalRequest();
  }

  Result<Stream> MakeStream() noexcept {
    auto configured = net::ApplyTcpOptions(fd_, tcp_options_);
    if (!configured.HasValue()) {
      return std::unexpected(configured.Error());
    }
    Stream stream(loop_, fd_, peer_);
    fd_ = -1;
    return stream;
  }

  void ReleasePhysicalRequest() noexcept {
    CancelTimer();
    if (fd_ >= 0) {
      (void)::close(std::exchange(fd_, -1));
    }
  }

  Loop* loop_;
  net::Endpoint peer_;
  net::TcpOptions tcp_options_;
  time::Duration timeout_;
  time::TimerId timer_{};
  int fd_{-1};
  bool timed_out_{false};
  bool connect_terminal_{false};
  bool cancel_submitted_{false};
  bool cancel_terminal_{false};
  backend::ValueResultState<Stream> result_;
};

coro::Task<Result<Stream>> ConnectResolved(Loop* loop, ConnectorOptions options,
                                           Result<net::Endpoint> peer) {
  if (!peer.HasValue()) {
    co_return std::unexpected(peer.Error());
  }
  co_return co_await ConnectAwaiter(loop, *peer, options.tcp_options, options.connect_timeout);
}

}  // namespace

namespace detail {

void DispatchConnectComplete(Op* op) noexcept {
  ConnectAwaiter::OnComplete(op);
}

CompletionDisposition DispatchConnectCancelComplete(Op* op) noexcept {
  return ConnectAwaiter::OnCancelComplete(op);
}

}  // namespace detail

Connector::Connector(Loop* loop, ConnectorOptions options) noexcept
    : loop_(loop), options_(options) {
  ALYRN_CHECK(loop_ != nullptr, "Connector: loop must not be null");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Connector created from wrong Loop thread");
}

Result<Connector> Connector::Create(Loop* loop, ConnectorOptions options) noexcept {
  if (loop == nullptr) {
    return std::unexpected(Errno(EINVAL));
  }
  return Connector{loop, options};
}

Connector::Connector(Connector&& other) noexcept
    : loop_(std::exchange(other.loop_, nullptr)), options_(std::exchange(other.options_, {})) {}

Connector& Connector::operator=(Connector&& other) noexcept {
  if (this != &other) {
    loop_ = std::exchange(other.loop_, nullptr);
    options_ = std::exchange(other.options_, {});
  }
  return *this;
}

coro::Task<Result<Stream>> Connector::Connect(net::Endpoint peer) {
  RequireOwnerLoop();
  return ConnectResolved(loop_, options_, Result<net::Endpoint>(std::in_place, peer));
}

coro::Task<Result<Stream>> Connector::Connect(std::string_view ip, std::uint16_t port) {
  RequireOwnerLoop();
  return ConnectResolved(loop_, options_, net::ParseIpAddress(ip, port));
}

coro::Task<void> Connector::SleepFor(time::Duration delay) {
  RequireOwnerLoop();
  auto result = co_await uring::SleepFor(*loop_, delay);
  (void)result;
}

void Connector::RequireOwnerLoop() const noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Connector operation has no owner Loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Connector operation called from wrong Loop thread");
}

}  // namespace alyrn::uring
