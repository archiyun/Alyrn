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
#include "coropact/base/try.h"
#include "coropact/luring/detail/close_state.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/op.h"
#include "coropact/luring/stream.h"
#include "coropact/net/endpoint.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"

namespace coropact::luring {

namespace {

using AcceptResult = base::Result<LUringStream>;

base::Result<int> CreatedListenFd(const net::Endpoint& listen_addr,
                                  const LUringListenOptions& options) noexcept {
  const int fd =
      ::socket(listen_addr.NativeFamily(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
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

  if (::bind(fd, listen_addr.SockAddr(), listen_addr.SockAddrLen()) < 0) {
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

bool IsMultishotUnsupported(int cqe_result) noexcept {
  return cqe_result == -EINVAL || cqe_result == -EOPNOTSUPP;
}

}  // namespace

class LUringAcceptSource::NextAwaiter {
public:
  explicit NextAwaiter(LUringAcceptSource& source) noexcept : source_(&source) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (source_->pending_next_ != nullptr) {
      result_.emplace(std::unexpected(base::MakeErrno(EBUSY)));
      COROPACT_IGNORE_RESULT(completion_gate_.TryComplete());
      return false;
    }

    continuation_.Bind(continuation);

    LUringAcceptSource::Result result;
    if (source_->TryTakeNext(result)) {
      result_.emplace(std::move(result));
      COROPACT_IGNORE_RESULT(completion_gate_.TryComplete());
      return false;
    }

    source_->pending_next_ = this;
    return true;
  }

  LUringAcceptSource::Result await_resume() noexcept {
    assert(result_.has_value());
    return std::move(*result_);
  }

  void Complete(LUringAcceptSource::Result result) noexcept {
    if (!completion_gate_.TryComplete()) {
      return;
    }
    result_.emplace(std::move(result));
    continuation_.Schedule();
  }

private:
  LUringAcceptSource* source_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  std::optional<LUringAcceptSource::Result> result_;
};

class LUringAcceptSource::StopAwaiter {
public:
  explicit StopAwaiter(LUringAcceptSource& source) noexcept : source_(&source) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    continuation_.Bind(continuation);
    source_->pending_stop_ = this;

    auto waiting = source_->BeginStop();
    if (!waiting.has_value()) {
      source_->pending_stop_ = nullptr;
      result_.emplace(std::unexpected(waiting.error()));
      COROPACT_IGNORE_RESULT(completion_gate_.TryComplete());
      return false;
    }

    if (!*waiting) {
      source_->pending_stop_ = nullptr;
      result_.emplace(base::Result<void>{});
      COROPACT_IGNORE_RESULT(completion_gate_.TryComplete());
      return false;
    }

    return true;
  }

  base::Result<void> await_resume() noexcept {
    assert(result_.has_value());
    return std::move(*result_);
  }

  void Complete(base::Result<void> result) noexcept {
    if (!completion_gate_.TryComplete()) {
      return;
    }
    result_.emplace(std::move(result));
    continuation_.Schedule();
  }

private:
  LUringAcceptSource* source_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  std::optional<base::Result<void>> result_;
};

LUringAcceptSource::LUringAcceptSource(LUringListener* listener,
                                       net::detail::AcceptSourceStateMachine state) noexcept
    : listener_(listener), state_(std::move(state)), accept_op_(this), cancel_op_(this) {}

LUringAcceptSource::~LUringAcceptSource() {
  if (listener_ == nullptr) {
    return;
  }

  COROPACT_DCHECK(listener_->loop_->IsInLoopThread(),
                  "LUringAcceptSource destroyed from wrong thread");
  COROPACT_DCHECK(pending_next_ == nullptr, "LUringAcceptSource destroyed with pending Next");
  COROPACT_DCHECK(pending_stop_ == nullptr, "LUringAcceptSource destroyed with pending Stop");
  COROPACT_DCHECK(!accept_submitted_, "LUringAcceptSource destroyed with active accept");
  COROPACT_DCHECK(!cancel_submitted_, "LUringAcceptSource destroyed with active cancel");

  if (listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }
}

LUringAcceptSource::LUringAcceptSource(LUringAcceptSource&& other) noexcept
    : listener_(std::exchange(other.listener_, nullptr)),
      state_(std::move(other.state_)),
      events_(std::move(other.events_)),
      terminal_error_(std::move(other.terminal_error_)),
      pending_next_(nullptr),
      pending_stop_(nullptr),
      accept_op_(this),
      cancel_op_(this),
      accept_submitted_(false),
      cancel_submitted_(false),
      multishot_enabled_(other.multishot_enabled_) {
  COROPACT_CHECK(other.pending_next_ == nullptr,
                 "LUringAcceptSource cannot move with pending Next");
  COROPACT_CHECK(other.pending_stop_ == nullptr,
                 "LUringAcceptSource cannot move with pending Stop");
  COROPACT_CHECK(!other.accept_submitted_, "LUringAcceptSource cannot move while active");
  COROPACT_CHECK(!other.cancel_submitted_, "LUringAcceptSource cannot move while cancelling");

  if (listener_ != nullptr && listener_->accept_source_ == &other) {
    listener_->accept_source_ = this;
  }
}

LUringAcceptSource& LUringAcceptSource::operator=(LUringAcceptSource&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  COROPACT_CHECK(pending_next_ == nullptr, "LUringAcceptSource destination has pending Next");
  COROPACT_CHECK(pending_stop_ == nullptr, "LUringAcceptSource destination has pending Stop");
  COROPACT_CHECK(!accept_submitted_, "LUringAcceptSource destination is active");
  COROPACT_CHECK(!cancel_submitted_, "LUringAcceptSource destination is cancelling");
  COROPACT_CHECK(other.pending_next_ == nullptr, "LUringAcceptSource source has pending Next");
  COROPACT_CHECK(other.pending_stop_ == nullptr, "LUringAcceptSource source has pending Stop");
  COROPACT_CHECK(!other.accept_submitted_, "LUringAcceptSource source is active");
  COROPACT_CHECK(!other.cancel_submitted_, "LUringAcceptSource source is cancelling");

  if (listener_ != nullptr && listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }

  listener_ = std::exchange(other.listener_, nullptr);
  state_ = std::move(other.state_);
  events_ = std::move(other.events_);
  terminal_error_ = std::move(other.terminal_error_);
  multishot_enabled_ = other.multishot_enabled_;

  if (listener_ != nullptr && listener_->accept_source_ == &other) {
    listener_->accept_source_ = this;
  }

  return *this;
}

base::Result<LUringStream> LUringAcceptSource::MakeStream(int accepted_fd) noexcept {
  sockaddr_storage peer{};
  socklen_t peer_len = sizeof(peer);

  if (::getpeername(accepted_fd, reinterpret_cast<sockaddr*>(&peer), &peer_len) < 0) {
    auto error = base::CurrentErrno();
    ::close(accepted_fd);
    return std::unexpected(error);
  }

  LUringStream stream(
      listener_->loop_, accepted_fd,
      net::Endpoint(reinterpret_cast<const sockaddr*>(&peer), peer_len));
  stream.SetZeroCopyWritesEnabled(listener_->zero_copy_writes_);
  stream.SetZeroCopyDiagnostics(listener_->zero_copy_diagnostics_);
  return stream;
}

base::Result<void> LUringAcceptSource::StartOperation() noexcept {
  if (listener_ == nullptr || listener_->closed_ || listener_->fd_ < 0) {
    return std::unexpected(base::MakeErrno(EBADF));
  }

  if (accept_submitted_) {
    return {};
  }

  if (!state_.CanArm() || !state_.TryArm()) {
    return {};
  }

  accept_op_.Prepare();
  ++listener_->pending_accepts_;

  auto submitted = listener_->loop_->SubmitOp(
      &accept_op_, [fd = listener_->fd_, multishot = multishot_enabled_](io_uring_sqe* sqe) noexcept {
        if (multishot) {
          io_uring_prep_multishot_accept(
              sqe, fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        } else {
          io_uring_prep_accept(
              sqe, fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        }
      });

  if (!submitted.has_value()) {
    --listener_->pending_accepts_;
    auto completed = state_.CompleteMultishotEvent(
        net::detail::EventDisposition::kNone, net::detail::MultishotRequestDisposition::kTerminal);
    assert(completed.has_value());
    return std::unexpected(submitted.error());
  }

  accept_submitted_ = true;
  return {};
}

base::Result<void> LUringAcceptSource::Start() noexcept {
  if (state_.State() != net::detail::AcceptSourceState::kIdle) {
    return std::unexpected(base::MakeErrno(EALREADY));
  }

  if (listener_ == nullptr || listener_->closed_ || listener_->fd_ < 0) {
    return std::unexpected(base::MakeErrno(EBADF));
  }

  if (listener_->pending_accepts_ != 0 || listener_->accept_source_ != nullptr) {
    return std::unexpected(base::MakeErrno(EBUSY));
  }

  auto started = state_.Start();
  if (!started.has_value()) {
    return started;
  }

  listener_->accept_source_ = this;

  auto submitted = StartOperation();
  if (!submitted.has_value()) {
    RequestBackendStop(submitted.error());
    return std::unexpected(submitted.error());
  }

  return {};
}

base::Result<void> LUringAcceptSource::StartCancel() noexcept {
  if (!accept_submitted_ || cancel_submitted_) {
    return {};
  }

  cancel_op_.Prepare();
  const auto target = reinterpret_cast<std::uint64_t>(&accept_op_);

  auto submitted = listener_->loop_->SubmitOp(&cancel_op_, [target](io_uring_sqe* sqe) noexcept {
    io_uring_prep_cancel64(sqe, target, IORING_ASYNC_CANCEL_ALL);
  });

  if (!submitted.has_value()) {
    return std::unexpected(submitted.error());
  }

  cancel_submitted_ = true;
  return {};
}

base::Result<bool> LUringAcceptSource::BeginStop() noexcept {
  auto stopped = state_.RequestStop();
  if (!stopped.has_value()) {
    return std::unexpected(stopped.error());
  }

  if (!accept_submitted_ && !cancel_submitted_) {
    return false;
  }

  if (accept_submitted_) {
    auto cancelled = StartCancel();
    if (!cancelled.has_value()) {
      return std::unexpected(cancelled.error());
    }
  }

  return true;
}

void LUringAcceptSource::RequestBackendStop(std::optional<base::Error> error) noexcept {
  if (error.has_value() && !terminal_error_.has_value()) {
    terminal_error_ = *error;
  }

  auto stopped = state_.RequestStop();
  assert(stopped.has_value());

  if (accept_submitted_ && !cancel_submitted_) {
    auto cancelled = StartCancel();
    if (!cancelled.has_value() && !terminal_error_.has_value()) {
      terminal_error_ = cancelled.error();
    }
  }
}

void LUringAcceptSource::EnsureSubmission() noexcept {
  if (listener_ == nullptr || listener_->closed_ ||
      state_.State() != net::detail::AcceptSourceState::kActive || accept_submitted_) {
    return;
  }

  auto submitted = StartOperation();
  if (!submitted.has_value()) {
    RequestBackendStop(submitted.error());
    DeliverNextIfReady();
  }
}

void LUringAcceptSource::OnCompletion(CompletionEvent event) noexcept {
  const bool request_still_active = event.More();
  const int cqe_res = event.result;

  if (!request_still_active) {
    accept_submitted_ = false;
    assert(listener_->pending_accepts_ > 0);
    --listener_->pending_accepts_;
  }

  bool produced_event = false;

  if (cqe_res >= 0) {
    if (state_.QueuedEvents() >= state_.Options().event_capacity) {
      ::close(cqe_res);
      RequestBackendStop(base::MakeErrno(ENOBUFS));
    } else {
      auto stream = MakeStream(cqe_res);
      if (!stream.has_value()) {
        RequestBackendStop(stream.error());
      } else {
        try {
          events_.push_back(std::move(*stream));
          produced_event = true;
        } catch (...) {
          RequestBackendStop(base::MakeErrno(ENOMEM));
        }
      }
    }
  } else if (!request_still_active) {
    const auto state = state_.State();
    const bool stopping = state == net::detail::AcceptSourceState::kStopping ||
                          state == net::detail::AcceptSourceState::kDraining ||
                          state == net::detail::AcceptSourceState::kTerminal;

    if (!stopping && multishot_enabled_ && IsMultishotUnsupported(cqe_res)) {
      // The opcode probe only proves that ACCEPT exists; it cannot prove that
      // the multishot flag is accepted by this kernel.  Treat the first
      // unsupported terminal CQE as a path-selection event and retry the same
      // logical source with ordinary one-shot accept.
      multishot_enabled_ = false;
    } else if (!stopping) {
      RequestBackendStop(base::MakeNegErrno(cqe_res));
    }
  }

  auto recorded = state_.CompleteMultishotEvent(
      produced_event ? net::detail::EventDisposition::kProduced
                     : net::detail::EventDisposition::kNone,
      request_still_active ? net::detail::MultishotRequestDisposition::kMore
                           : net::detail::MultishotRequestDisposition::kTerminal);
  if (!recorded.has_value()) {
    RequestBackendStop(recorded.error());
  }

  if (!request_still_active) {
    accept_op_.BeginNextRequest();
  }

  if (request_still_active && state_.State() == net::detail::AcceptSourceState::kActive &&
      state_.QueuedEvents() >= state_.Options().event_capacity) {
    RequestBackendStop(base::MakeErrno(ENOBUFS));
  }

  if (!request_still_active && state_.State() == net::detail::AcceptSourceState::kActive &&
      !terminal_error_.has_value()) {
    EnsureSubmission();
  }

  DeliverNextIfReady();

  if (!request_still_active) {
    listener_->NotifyCloseProgress();
  }

  CompleteStopIfReady();
}

void LUringAcceptSource::OnCancelComplete(int cqe_res) noexcept {
  cancel_submitted_ = false;

  if (cqe_res < 0 && cqe_res != -ENOENT && cqe_res != -ECANCELED && !terminal_error_.has_value()) {
    terminal_error_ = base::MakeNegErrno(cqe_res);
  }

  CompleteStopIfReady();
}

void LUringAcceptSource::OnListenerClosed() noexcept {
  auto stopped = state_.RequestStop();
  assert(stopped.has_value());
  DeliverNextIfReady();
  CompleteStopIfReady();
}

bool LUringAcceptSource::TryTakeNext(Result& result) noexcept {
  if (!events_.empty()) {
    Event event(std::in_place, std::move(events_.front()));
    events_.pop_front();

    const bool consumed = state_.ConsumeEvent();
    assert(consumed);

    result = Result(std::in_place, std::move(event));
    EnsureSubmission();
    return true;
  }

  if (state_.State() == net::detail::AcceptSourceState::kTerminal) {
    if (terminal_error_.has_value()) {
      result = std::unexpected(*terminal_error_);
    } else {
      result = Result(std::in_place, std::nullopt);
    }

    ReleaseListenerReservation();
    return true;
  }

  return false;
}

void LUringAcceptSource::DeliverNextIfReady() noexcept {
  if (pending_next_ == nullptr) {
    return;
  }

  Result result;
  if (!TryTakeNext(result)) {
    return;
  }

  auto* awaiter = std::exchange(pending_next_, nullptr);
  awaiter->Complete(std::move(result));
}

void LUringAcceptSource::CompleteStopIfReady() noexcept {
  // The cancel request has its own operation object and CQE.  The target
  // accept may reach its terminal CQE before the cancel CQE, so Stop() must
  // not resume (and let the source be destroyed) until both operations have
  // left the ring.
  if (pending_stop_ == nullptr || accept_submitted_ || cancel_submitted_) {
    return;
  }

  auto* awaiter = std::exchange(pending_stop_, nullptr);
  awaiter->Complete(base::Result<void>{});
}

void LUringAcceptSource::ReleaseListenerReservation() noexcept {
  if (listener_ != nullptr && state_.State() == net::detail::AcceptSourceState::kTerminal &&
      listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }
}

coro::Task<LUringAcceptSource::Result> LUringAcceptSource::Next() {
  if (listener_ == nullptr) {
    co_return std::unexpected(base::MakeErrno(EBADF));
  }
  if (!listener_->loop_->IsInLoopThread()) {
    co_return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (pending_next_ != nullptr) {
    co_return std::unexpected(base::MakeErrno(EBUSY));
  }

  if (state_.State() == net::detail::AcceptSourceState::kIdle) {
    auto started = Start();
    if (!started.has_value()) {
      co_return std::unexpected(started.error());
    }
  }

  Result result;
  if (TryTakeNext(result)) {
    co_return result;
  }

  co_return co_await NextAwaiter(*this);
}

coro::Task<base::Result<void>> LUringAcceptSource::Stop() {
  if (listener_ == nullptr) {
    co_return base::Result<void>{};
  }
  if (!listener_->loop_->IsInLoopThread()) {
    co_return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (pending_stop_ != nullptr) {
    co_return std::unexpected(base::MakeErrno(EBUSY));
  }

  if (!accept_submitted_ && !cancel_submitted_) {
    auto stopped = state_.RequestStop();
    if (!stopped.has_value()) {
      co_return std::unexpected(stopped.error());
    }
    DeliverNextIfReady();
    ReleaseListenerReservation();
    co_return base::Result<void>{};
  }

  co_return co_await StopAwaiter(*this);
}

class LUringListener::AcceptAwaiter : public detail::LUringOpHook<LUringListener::AcceptAwaiter> {
  friend void detail::DispatchAcceptComplete(LUringOp* op) noexcept;

public:
  using OpHook = detail::LUringOpHook<AcceptAwaiter>;

  explicit AcceptAwaiter(LUringListener& listener) noexcept
      : OpHook(LUringOpKind::kAcceptComplete), listener_(&listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (listener_->closed_ || listener_->fd_ < 0) {
      immediate_.emplace(std::unexpected(base::MakeErrno(EBADF)));
      return false;
    }
    if (listener_->accept_source_ != nullptr) {
      immediate_.emplace(std::unexpected(base::MakeErrno(EBUSY)));
      return false;
    }
    ++listener_->pending_accepts_;
    Op()->kind = LUringOpKind::kAcceptComplete;
    Op()->resume_work.SetHandle(continuation);
    peer_len_ = static_cast<socklen_t>(sizeof(peer_addr_));

    auto submitted =
        listener_->loop_->SubmitOp(Op(), [this, fd = listener_->fd_](io_uring_sqe* sqe) noexcept {
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
    auto* self = static_cast<OpHook*>(op)->Owner();
    if (self->listener_ != nullptr) {
      LUringListener* listener = self->listener_;
      assert(listener->pending_accepts_ > 0);
      --listener->pending_accepts_;

      if (!op->result.HasValue()) {
        self->immediate_ = std::unexpected(op->result.Error());
      } else if (*op->result < 0) {
        self->immediate_ = std::unexpected(base::MakeNegErrno(*op->result));
      } else {
        self->immediate_ =
            MakeStream(listener->loop_, *op->result, self->peer_addr_, self->peer_len_);
        if (self->immediate_->has_value()) {
          self->immediate_->value().SetZeroCopyWritesEnabled(
              listener->zero_copy_writes_);
          self->immediate_->value().SetZeroCopyDiagnostics(
              listener->zero_copy_diagnostics_);
        }
      }

      self->listener_ = nullptr;
      listener->NotifyCloseProgress();
    }
  }

  LUringOp* Op() noexcept { return static_cast<OpHook*>(this); }

  LUringListener* listener_;
  sockaddr_storage peer_addr_{};
  socklen_t peer_len_{sizeof(peer_addr_)};
  std::optional<AcceptResult> immediate_;
};

class LUringListener::CloseAwaiter : public detail::LUringOpHook<LUringListener::CloseAwaiter> {
  friend void detail::DispatchListenerCloseComplete(LUringOp* op) noexcept;

public:
  using OpHook = detail::LUringOpHook<CloseAwaiter>;

  explicit CloseAwaiter(LUringListener& listener) noexcept
      : OpHook(LUringOpKind::kListenerCloseComplete), listener_(&listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (listener_->pending_close_ != nullptr) {
      state_.SetError(base::MakeErrno(EBUSY));
      return false;
    }
    if (listener_->closed_ || listener_->fd_ < 0) {
      state_.SetSuccess();
      return false;
    }

    listener_->closed_ = true;
    if (listener_->accept_source_ != nullptr) {
      listener_->accept_source_->OnListenerClosed();
    }
    if (listener_->pending_accepts_ == 0) {
      state_.SetResult(CloseFd());
      return false;
    }

    listener_->pending_close_ = this;
    continuation_ = continuation;
    Op()->kind = LUringOpKind::kListenerCloseComplete;

    auto submitted =
        listener_->loop_->SubmitOp(Op(), [fd = listener_->fd_](io_uring_sqe* sqe) noexcept {
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
    Op()->resume_work.SetHandle(continuation_);
    if (current != Op()) {
      loop->ScheduleCompletion(&Op()->resume_work);
    }
  }

private:
  static void OnCancelComplete(LUringOp* op) noexcept {
    auto* self = static_cast<OpHook*>(op)->Owner();
    self->state_.MarkCancelCompleted();
    self->TryComplete(op);
  }

  LUringOp* Op() noexcept { return static_cast<OpHook*>(this); }

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

void DispatchAcceptSourceComplete(LUringOp* op, CompletionEvent event) noexcept {
  auto* operation = static_cast<LUringAcceptSource::AcceptOperation*>(op);
  operation->Source()->OnCompletion(event);
}

void DispatchAcceptSourceCancelComplete(LUringOp* op) noexcept {
  auto* operation = static_cast<LUringAcceptSource::CancelOperation*>(op);

  int result = -EIO;
  if (op->result.HasValue()) {
    result = *op->result;
  }
  operation->Source()->OnCancelComplete(result);
}

}  // namespace detail

base::Result<LUringListener> LUringListener::Create(LUringLoop* loop,
                                                    const net::Endpoint& listen_addr,
                                                    LUringListenOptions options) noexcept {
  assert(loop != nullptr);
  assert(loop->IsInLoopThread());

  return LUringListener(
      loop, COROPACT_TRY(CreatedListenFd(listen_addr, options)),
      options.zero_copy_writes, options.zero_copy_diagnostics);
}

LUringListener::LUringListener(
    LUringLoop* loop,
    int fd,
    bool zero_copy_writes,
    ZeroCopySendDiagnostics* zero_copy_diagnostics) noexcept
    : loop_(loop),
      fd_(fd),
      zero_copy_writes_(zero_copy_writes),
      zero_copy_diagnostics_(zero_copy_diagnostics) {
  assert(loop_ != nullptr);
  assert(fd_ >= 0);
}

LUringListener::LUringListener(LUringListener&& other) noexcept
    : loop_(PrepareMove(other)),
      fd_(std::exchange(other.fd_, -1)),
      pending_accepts_(0),
      pending_close_(nullptr),
      accept_source_(nullptr),
      zero_copy_writes_(other.zero_copy_writes_),
      zero_copy_diagnostics_(other.zero_copy_diagnostics_),
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
  accept_source_ = nullptr;
  zero_copy_writes_ = other.zero_copy_writes_;
  zero_copy_diagnostics_ = other.zero_copy_diagnostics_;
  closed_ = other.closed_;
  other.closed_ = true;
  return *this;
}

LUringListener::~LUringListener() {
  assert(pending_accepts_ == 0);
  assert(pending_close_ == nullptr);
  assert(accept_source_ == nullptr);
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
    return std::unexpected(base::MakeErrno(EBADF));
  }
  return GetLocalAddress(fd_);
}

base::Result<LUringAcceptSource> LUringListener::AcceptSource(
    net::AcceptSourceOptions options) noexcept {
  if (closed_ || fd_ < 0) {
    return std::unexpected(base::MakeErrno(EBADF));
  }
  if (pending_accepts_ != 0 || pending_close_ != nullptr || accept_source_ != nullptr) {
    return std::unexpected(base::MakeErrno(EBUSY));
  }

  auto state = COROPACT_TRY(net::detail::AcceptSourceStateMachine::Create(options));
  return LUringAcceptSource(this, std::move(state));
}

void LUringListener::NotifyCloseProgress() noexcept {
  if (pending_close_ != nullptr) {
    pending_close_->TryComplete();
  }
}

void LUringListener::ResetForMove() noexcept {
  COROPACT_CHECK(loop_ != nullptr, "LUringListener move destination is not initialized");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "LUringListener move called from wrong LUringLoop thread");
  COROPACT_CHECK(pending_accepts_ == 0,
                 "LUringListener move destination has pending accept operations");
  COROPACT_CHECK(pending_close_ == nullptr,
                 "LUringListener move destination has a pending close operation");
  COROPACT_CHECK(accept_source_ == nullptr,
                 "LUringListener move destination has an active AcceptSource");

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
  COROPACT_CHECK(other.accept_source_ == nullptr,
                 "LUringListener cannot move with an active AcceptSource");

  LUringLoop* loop = other.loop_;
  other.loop_ = nullptr;
  return loop;
}

}  // namespace coropact::luring
