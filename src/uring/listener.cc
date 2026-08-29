#include "alyrn/uring/listener.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <cstdint>
#include <expected>
#include <optional>
#include <utility>

#include "alyrn/backend/value_result_state.h"
#include "alyrn/backend/loop.h"
#include "alyrn/detail/check.h"
#include "alyrn/coro/task.h"
#include "alyrn/detail/uring/cancel_result.h"
#include "alyrn/detail/uring/fd_close_convergence.h"
#include "alyrn/detail/uring/loop_access.h"
#include "alyrn/detail/uring/op.h"
#include "alyrn/detail/uring/op_hook.h"
#include "alyrn/detail/uring/operation_submission.h"
#include "alyrn/detail/uring/sqe_prep.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/stream.h"
#include "alyrn/detail/net/accept_source_state.h"
#include "alyrn/detail/net/source_state.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/detail/net/socket.h"
#include "alyrn/detail/operation/completion_gate.h"
#include "alyrn/detail/operation/scheduler_continuation.h"
#include "alyrn/result.h"

namespace alyrn::uring {

using namespace detail;
using namespace net::detail;

namespace {

using AcceptResult = Result<Stream>;

Result<int> CreatedListenFd(const net::Endpoint& listen_addr,
                            const ListenOptions& options) noexcept {
  const int fd =
      ::socket(listen_addr.NativeFamily(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(CurrentErrno());
  }

  auto fail = [fd](Error error) -> Result<int> {
    ::close(fd);
    return std::unexpected(error);
  };

  int on = 1;

  if (options.reuse_addr) {
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
      return fail(CurrentErrno());
    }
  }

  if (options.reuse_port) {
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on)) < 0) {
      return fail(CurrentErrno());
    }
  }

  if (::bind(fd, listen_addr.SockAddr(), listen_addr.SockAddrLen()) < 0) {
    return fail(CurrentErrno());
  }

  if (::listen(fd, options.backlog) < 0) {
    return fail(CurrentErrno());
  }

  return fd;
}

Result<net::Endpoint> GetLocalAddress(int fd) noexcept {
  sockaddr_storage addr{};
  auto len = static_cast<socklen_t>(sizeof(addr));
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    return std::unexpected(CurrentErrno());
  }
  return net::Endpoint(reinterpret_cast<const sockaddr*>(&addr), len);
}

AcceptResult MakeStream(Loop* loop, int fd, const sockaddr_storage& peer_addr,
                        socklen_t peer_len, const net::TcpOptions& tcp_options) noexcept {
  auto configured = net::ApplyTcpOptions(fd, tcp_options);
  if (!configured.has_value()) {
    (void)::close(fd);
    return std::unexpected(configured.error());
  }
  return Stream(loop, fd, net::Endpoint(reinterpret_cast<const sockaddr*>(&peer_addr), peer_len));
}

bool IsMultishotUnsupported(int cqe_result) noexcept {
  return cqe_result == -EINVAL || cqe_result == -EOPNOTSUPP;
}

}  // namespace

// --- NextAwaiter ---
bool AcceptSource::NextAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (source_->listener_ == nullptr) {
    result_.SetError(Errno(EBADF));
    (void)(completion_gate_.TryComplete());
    return false;
  }
  if (!source_->listener_->loop_->IsInLoopThread()) {
    result_.SetError(Errno(EINVAL));
    (void)(completion_gate_.TryComplete());
    return false;
  }
  if (source_->pending_next_ != nullptr) {
    result_.SetError(Errno(EBUSY));
    (void)(completion_gate_.TryComplete());
    return false;
  }

  if (source_->state_.State() == AcceptSourceState::kIdle) {
    auto started = source_->Start();
    if (!started.has_value()) {
      result_.SetError(started.error());
      (void)(completion_gate_.TryComplete());
      return false;
    }
  }

  NextResult result;
  if (source_->TryTakeNext(result)) {
    result_.SetResult(std::move(result));
    (void)(completion_gate_.TryComplete());
    return false;
  }

  continuation_.Bind(continuation);
  source_->pending_next_ = this;
  return true;
}

AcceptSource::NextResult AcceptSource::NextAwaiter::await_resume() noexcept {
  return result_.Take();
}

void AcceptSource::NextAwaiter::Complete(NextResult result) noexcept {
  if (!completion_gate_.TryComplete()) {
    return;
  }
  result_.SetResult(std::move(result));
  detail::LoopAccess::ScheduleCompletion(*source_->listener_->loop_, continuation_);
}

// --- StopAwaiter ---
class [[nodiscard]] AcceptSource::StopAwaiter {
public:
  explicit StopAwaiter(AcceptSource& source) noexcept : source_(&source) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    continuation_.Bind(continuation);
    source_->pending_stop_ = this;

    auto waiting = source_->BeginStop();
    if (!waiting.has_value()) {
      source_->pending_stop_ = nullptr;
      result_.emplace(std::unexpected(waiting.error()));
      (void)(completion_gate_.TryComplete());
      return false;
    }

    if (!*waiting) {
      source_->pending_stop_ = nullptr;
      result_.emplace(Result<void>{});
      (void)(completion_gate_.TryComplete());
      return false;
    }

    return true;
  }

  Result<void> await_resume() noexcept {
    ALYRN_CHECK(result_.has_value(), "Uring accept source Stop resumed without a result");
    return *result_;
  }

  void Complete(Result<void> result) noexcept {
    if (!completion_gate_.TryComplete()) {
      return;
    }
    result_.emplace(result);
    continuation_.Schedule();
  }

private:
  AcceptSource* source_;
  ::alyrn::detail::operation::SchedulerContinuation continuation_;
  ::alyrn::detail::operation::CompletionGate completion_gate_;
  std::optional<Result<void>> result_;
};

AcceptSource::AcceptSource(Listener* listener, AcceptSourceStateMachine state) noexcept
    : listener_(listener), state_(state), accept_op_(this), cancel_op_(this) {}

AcceptSource::~AcceptSource() {
  if (listener_ == nullptr) {
    return;
  }

  ALYRN_CHECK(listener_->loop_->IsInLoopThread(), "AcceptSource destroyed from wrong thread");
  ALYRN_CHECK(pending_next_ == nullptr, "AcceptSource destroyed with pending Next");
  ALYRN_CHECK(pending_stop_ == nullptr, "AcceptSource destroyed with pending Stop");
  ALYRN_CHECK(!accept_submitted_, "AcceptSource destroyed with active accept");
  ALYRN_CHECK(!cancel_submitted_, "AcceptSource destroyed with active cancel");
  const auto state = state_.State();
  ALYRN_CHECK(state == AcceptSourceState::kIdle || state == AcceptSourceState::kDraining ||
                     state == AcceptSourceState::kTerminal,
                 "AcceptSource destroyed before reaching a safe lifecycle state");

  if (listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }
}

AcceptSource::AcceptSource(AcceptSource&& other) noexcept
    : listener_(std::exchange(other.listener_, nullptr)),
      state_(other.state_),
      events_(std::move(other.events_)),
      terminal_error_(other.terminal_error_),
      accept_op_(this),
      cancel_op_(this),
      multishot_enabled_(other.multishot_enabled_) {
  ALYRN_CHECK(other.pending_next_ == nullptr, "AcceptSource cannot move with pending Next");
  ALYRN_CHECK(other.pending_stop_ == nullptr, "AcceptSource cannot move with pending Stop");
  ALYRN_CHECK(!other.accept_submitted_, "AcceptSource cannot move while active");
  ALYRN_CHECK(!other.cancel_submitted_, "AcceptSource cannot move while cancelling");

  if (listener_ != nullptr && listener_->accept_source_ == &other) {
    listener_->accept_source_ = this;
  }
}

AcceptSource& AcceptSource::operator=(AcceptSource&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  ALYRN_CHECK(pending_next_ == nullptr, "AcceptSource destination has pending Next");
  ALYRN_CHECK(pending_stop_ == nullptr, "AcceptSource destination has pending Stop");
  ALYRN_CHECK(!accept_submitted_, "AcceptSource destination is active");
  ALYRN_CHECK(!cancel_submitted_, "AcceptSource destination is cancelling");
  ALYRN_CHECK(other.pending_next_ == nullptr, "AcceptSource source has pending Next");
  ALYRN_CHECK(other.pending_stop_ == nullptr, "AcceptSource source has pending Stop");
  ALYRN_CHECK(!other.accept_submitted_, "AcceptSource source is active");
  ALYRN_CHECK(!other.cancel_submitted_, "AcceptSource source is cancelling");

  if (listener_ != nullptr && listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }

  listener_ = std::exchange(other.listener_, nullptr);
  state_ = other.state_;
  events_ = std::move(other.events_);
  terminal_error_ = other.terminal_error_;
  multishot_enabled_ = other.multishot_enabled_;

  if (listener_ != nullptr && listener_->accept_source_ == &other) {
    listener_->accept_source_ = this;
  }

  return *this;
}

Result<Stream> AcceptSource::MakeStream(int accepted_fd) noexcept {
  auto configured = net::ApplyTcpOptions(accepted_fd, listener_->tcp_options_);
  if (!configured.has_value()) {
    (void)::close(accepted_fd);
    return std::unexpected(configured.error());
  }

  sockaddr_storage peer{};
  socklen_t peer_len = sizeof(peer);

  if (::getpeername(accepted_fd, reinterpret_cast<sockaddr*>(&peer), &peer_len) < 0) {
    auto error = CurrentErrno();
    ::close(accepted_fd);
    return std::unexpected(error);
  }

  Stream stream(listener_->loop_, accepted_fd,
                net::Endpoint(reinterpret_cast<const sockaddr*>(&peer), peer_len));
  stream.SetZeroCopyWritesEnabled(listener_->zero_copy_writes_);
  return stream;
}

Result<void> AcceptSource::StartOperation() noexcept {
  if (listener_ == nullptr || listener_->closed_ || listener_->fd_ < 0) {
    return std::unexpected(Errno(EBADF));
  }

  if (listener_->loop_->State() == backend::LoopState::kStopping ||
      listener_->loop_->State() == backend::LoopState::kStopped) {
    return std::unexpected(Errno(ECANCELED));
  }

  if (accept_submitted_) {
    return {};
  }

  if (!state_.CanArm() || !state_.TryArm()) {
    return {};
  }

  accept_op_.Prepare();
  ++listener_->pending_accepts_;

  auto submitted =
      detail::LoopAccess::SubmitOp(*listener_->loop_, &accept_op_,
                                   detail::PrepareAcceptSource(listener_->fd_, multishot_enabled_,
                                                               SOCK_NONBLOCK | SOCK_CLOEXEC));

  if (!submitted.has_value()) {
    --listener_->pending_accepts_;
    const auto completed = state_.CompleteMultishotEvent(EventDisposition::kNone,
                                                         MultishotRequestDisposition::kTerminal);
    (void)(completed);
    ALYRN_CHECK(completed.has_value(),
                   "Uring accept source failed to record terminal submit failure");
    return std::unexpected(submitted.error());
  }

  accept_submitted_ = true;
  return {};
}

Result<void> AcceptSource::Start() noexcept {
  if (state_.State() != AcceptSourceState::kIdle) {
    return std::unexpected(Errno(EALREADY));
  }

  if (listener_ == nullptr || listener_->closed_ || listener_->fd_ < 0) {
    return std::unexpected(Errno(EBADF));
  }

  if (listener_->pending_accepts_ != 0 || listener_->accept_source_ != nullptr) {
    return std::unexpected(Errno(EBUSY));
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

Result<void> AcceptSource::StartCancel() noexcept {
  if (!accept_submitted_ || cancel_submitted_) {
    return {};
  }

  cancel_op_.Prepare();
  const auto target = reinterpret_cast<std::uint64_t>(&accept_op_);

  auto submitted = detail::LoopAccess::SubmitOp(*listener_->loop_, &cancel_op_,
                                                detail::PrepareCancelAllByUserData(target));

  if (!submitted.has_value()) {
    return std::unexpected(submitted.error());
  }

  cancel_submitted_ = true;
  return {};
}

Result<bool> AcceptSource::BeginStop() noexcept {
  state_.RequestStop();

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

void AcceptSource::RequestBackendStop(std::optional<Error> error) noexcept {
  if (error.has_value() && !terminal_error_.has_value()) {
    terminal_error_ = *error;
  }

  state_.RequestStop();

  if (accept_submitted_ && !cancel_submitted_) {
    auto cancelled = StartCancel();
    if (!cancelled.has_value() && !terminal_error_.has_value()) {
      terminal_error_ = cancelled.error();
    }
  }
}

void AcceptSource::RequestBackendPause() noexcept {
  (void)(state_.RequestPause());

  if (accept_submitted_ && !cancel_submitted_) {
    auto cancelled = StartCancel();
    if (!cancelled.has_value()) {
      RequestBackendStop(cancelled.error());
    }
  }
}

void AcceptSource::EnsureSubmission() noexcept {
  if (listener_ == nullptr || listener_->closed_ || state_.State() != AcceptSourceState::kActive ||
      accept_submitted_ || cancel_submitted_) {
    return;
  }

  auto submitted = StartOperation();
  if (!submitted.has_value()) {
    RequestBackendStop(submitted.error());
    DeliverNextIfReady();
  }
}

void AcceptSource::MaybeResume() noexcept {
  if (terminal_error_.has_value() || listener_ == nullptr || listener_->closed_ ||
      cancel_submitted_) {
    return;
  }

  if (state_.TryResume()) {
    EnsureSubmission();
  }
}

// Translates an io_uring CQE into a logical source transition.
CompletionDisposition AcceptSource::OnCompletion(CompletionEvent event) noexcept {
  const bool request_still_active = event.More();
  const int cqe_res = event.result;

  if (!request_still_active) {
    accept_submitted_ = false;
    ALYRN_CHECK(listener_->pending_accepts_ > 0,
                   "Uring accept source pending-accept count underflow");
    --listener_->pending_accepts_;
  }

  bool produced_event = false;

  if (cqe_res >= 0) {
    if (state_.QueuedEvents() >= state_.Options().event_capacity) {
      ::close(cqe_res);
      RequestBackendPause();
    } else {
      auto stream = MakeStream(cqe_res);
      if (!stream.has_value()) {
        RequestBackendStop(stream.error());
      } else {
        try {
          events_.push_back(std::move(*stream));
          produced_event = true;
        } catch (...) {
          RequestBackendStop(Errno(ENOMEM));
        }
      }
    }
  } else if (!request_still_active) {
    const auto state = state_.State();
    const bool stopping =
        state == AcceptSourceState::kStopping || state == AcceptSourceState::kPausing ||
        state == AcceptSourceState::kPaused || state == AcceptSourceState::kDraining ||
        state == AcceptSourceState::kTerminal;

    if (!stopping && multishot_enabled_ && IsMultishotUnsupported(cqe_res)) {
      // The opcode probe only proves that ACCEPT exists; it cannot prove that
      // the multishot flag is accepted by this kernel.  Treat the first
      // unsupported terminal CQE as a path-selection event and retry the same
      // logical source with ordinary one-shot accept.
      multishot_enabled_ = false;
    } else if (!stopping) {
      RequestBackendStop(NegErrno(cqe_res));
    }
  }

  auto recorded = state_.CompleteMultishotEvent(
      produced_event ? EventDisposition::kProduced : EventDisposition::kNone,
      request_still_active ? MultishotRequestDisposition::kMore
                           : MultishotRequestDisposition::kTerminal);
  if (!recorded.has_value()) {
    RequestBackendStop(recorded.error());
  }

  if (!request_still_active) {
    accept_op_.BeginNextRequest();
  }

  if (state_.State() == AcceptSourceState::kActive &&
      state_.QueuedEvents() >= state_.Options().event_capacity) {
    RequestBackendPause();
  }

  if (!request_still_active && state_.State() == AcceptSourceState::kActive &&
      !terminal_error_.has_value()) {
    EnsureSubmission();
  }

  DeliverNextIfReady();
  MaybeResume();

  if (!request_still_active) {
    listener_->NotifyCloseProgress();
  }

  CompleteStopIfReady();

  return CompletionDisposition{
      .kernel_request_terminal = !request_still_active,
      .decrement_inflight = !request_still_active,
      .resume_continuation = false,
  };
}

void AcceptSource::OnCancelComplete(int cqe_res) noexcept {
  cancel_submitted_ = false;

  if (!detail::IsExpectedCancelCqeResult(cqe_res) && !terminal_error_.has_value()) {
    terminal_error_ = NegErrno(cqe_res);
  }

  MaybeResume();
  CompleteStopIfReady();
}

void AcceptSource::OnListenerClosed() noexcept {
  state_.RequestStop();
  DeliverNextIfReady();
  CompleteStopIfReady();
}

bool AcceptSource::TryTakeNext(NextResult& result) noexcept {
  if (!events_.empty()) {
    Event event(std::in_place, std::move(events_.front()));
    events_.pop_front();

    ALYRN_CHECK(state_.ConsumeEvent(), "AcceptSource: queue and state became inconsistent");

    result = NextResult(std::in_place, std::move(event));
    if (state_.State() == AcceptSourceState::kPaused) {
      MaybeResume();
    } else {
      EnsureSubmission();
    }
    return true;
  }

  if (state_.State() == AcceptSourceState::kTerminal) {
    if (terminal_error_.has_value()) {
      result = std::unexpected(*terminal_error_);
    } else {
      result = NextResult(std::in_place, std::nullopt);
    }

    ReleaseListenerReservation();
    return true;
  }

  return false;
}

void AcceptSource::DeliverNextIfReady() noexcept {
  if (pending_next_ == nullptr) {
    return;
  }

  NextResult result;
  if (!TryTakeNext(result)) {
    return;
  }

  auto* awaiter = std::exchange(pending_next_, nullptr);
  awaiter->Complete(std::move(result));
}

void AcceptSource::CompleteStopIfReady() noexcept {
  // The cancel request has its own operation object and CQE.  The target
  // accept may reach its terminal CQE before the cancel CQE, so Stop() must
  // not resume (and let the source be destroyed) until both operations have
  // left the ring.
  if (pending_stop_ == nullptr || accept_submitted_ || cancel_submitted_) {
    return;
  }

  auto* awaiter = std::exchange(pending_stop_, nullptr);
  awaiter->Complete(Result<void>{});
}

void AcceptSource::ReleaseListenerReservation() noexcept {
  if (listener_ != nullptr && state_.State() == AcceptSourceState::kTerminal &&
      listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }
}

coro::Task<Result<void>> AcceptSource::Stop() {
  if (listener_ == nullptr) {
    co_return Result<void>{};
  }
  if (!listener_->loop_->IsInLoopThread()) {
    co_return std::unexpected(Errno(EINVAL));
  }
  if (pending_stop_ != nullptr) {
    co_return std::unexpected(Errno(EBUSY));
  }

  if (!accept_submitted_ && !cancel_submitted_) {
    state_.RequestStop();
    DeliverNextIfReady();
    ReleaseListenerReservation();
    co_return Result<void>{};
  }

  co_return co_await StopAwaiter(*this);
}

// --- AcceptAwaiter ---
class [[nodiscard]] Listener::AcceptAwaiter : public detail::OpHook<Listener::AcceptAwaiter> {
  friend void detail::DispatchAcceptComplete(::alyrn::uring::detail::Op* op) noexcept;

public:
  using OpHook = detail::OpHook<AcceptAwaiter>;

  explicit AcceptAwaiter(Listener& listener) noexcept
      : OpHook(OpKind::kAcceptComplete), listener_(&listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    listener_->RequireOwnerLoop();
    if (listener_->closed_ || listener_->fd_ < 0) {
      CompleteInline(std::unexpected(Errno(EBADF)));
      return false;
    }
    if (listener_->accept_source_ != nullptr) {
      CompleteInline(std::unexpected(Errno(EBUSY)));
      return false;
    }
    ++listener_->pending_accepts_;
    listener_reservation_ = true;
    Operation()->kind = OpKind::kAcceptComplete;
    peer_len_ = static_cast<socklen_t>(sizeof(peer_addr_));

    return detail::SubmitAwaitingOperation(
        *listener_->loop_, *Operation(), continuation,
        detail::PrepareAccept(listener_->fd_, reinterpret_cast<sockaddr*>(&peer_addr_), &peer_len_,
                              SOCK_NONBLOCK | SOCK_CLOEXEC),
        [this](Error error) noexcept { CompleteInline(std::unexpected(error)); });
  }

  AcceptResult await_resume() noexcept { return result_.Take(); }

private:
  static void OnComplete(::alyrn::uring::detail::Op* op) noexcept {
    auto* self = OpHook::OwnerFrom(op);
    ALYRN_CHECK(self->listener_ != nullptr, "Uring Accept CQE has no listener owner");
    ALYRN_CHECK(self->listener_reservation_,
                   "Uring Accept CQE arrived without a listener reservation");
    ALYRN_CHECK(op->result.HasValue(), "Uring Accept CQE is missing its result");

    Listener* listener = self->listener_;
    if (*op->result < 0) {
      self->result_.SetError(NegErrno(*op->result));
    } else {
      auto result = MakeStream(listener->loop_, *op->result, self->peer_addr_, self->peer_len_,
                               listener->tcp_options_);
      if (result.has_value()) {
        result->SetZeroCopyWritesEnabled(listener->zero_copy_writes_);
      }
      self->result_.SetResult(std::move(result));
    }

    ALYRN_CHECK(op->TryAuthorizeCoupledResult(), "Uring Accept result was authorized twice");
    ALYRN_CHECK(op->TryAuthorizeCoupledRelease(),
                   "Uring Accept release was not authorized after its result");
    self->ReleaseListenerReservation();
  }

  void CompleteInline(AcceptResult result) noexcept {
    result_.SetResult(std::move(result));
    ALYRN_CHECK(Operation()->TryAuthorizeCoupledResult(),
                   "Uring Accept result was authorized twice");
    ALYRN_CHECK(Operation()->TryAuthorizeCoupledRelease(),
                   "Uring Accept release was not authorized after its result");
    ReleaseListenerReservation();
  }

  void ReleaseListenerReservation() noexcept {
    Listener* listener = std::exchange(listener_, nullptr);
    if (!listener_reservation_) {
      return;
    }

    listener_reservation_ = false;
    ALYRN_CHECK(listener != nullptr, "Uring Accept lost its listener reservation owner");
    ALYRN_CHECK(listener->pending_accepts_ > 0, "Uring Accept pending-accept count underflow");
    --listener->pending_accepts_;
    listener->NotifyCloseProgress();
  }

  Listener* listener_;
  sockaddr_storage peer_addr_{};
  socklen_t peer_len_{sizeof(peer_addr_)};
  backend::ValueResultState<Stream> result_;
  bool listener_reservation_{false};
};

// --- CloseAwaiter ---
class [[nodiscard]] Listener::CloseAwaiter : public detail::OpHook<Listener::CloseAwaiter> {
  friend void detail::DispatchListenerCloseComplete(::alyrn::uring::detail::Op* op) noexcept;

public:
  using OpHook = detail::OpHook<CloseAwaiter>;

  explicit CloseAwaiter(Listener& listener) noexcept
      : OpHook(OpKind::kListenerCloseComplete), listener_(&listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    listener_->RequireOwnerLoop();
    if (listener_->pending_close_ != nullptr) {
      convergence_.SetError(Errno(EBUSY));
      return false;
    }
    if (listener_->closed_ || listener_->fd_ < 0) {
      convergence_.SetSuccess();
      return false;
    }

    if (listener_->pending_accepts_ == 0) {
      listener_->closed_ = true;
      if (listener_->accept_source_ != nullptr) {
        listener_->accept_source_->OnListenerClosed();
      }
      convergence_.SetResult(CloseFd());
      return false;
    }

    listener_->pending_close_ = this;
    convergence_.BeginWaiting(continuation);
    Operation()->kind = OpKind::kListenerCloseComplete;

    auto submitted = detail::LoopAccess::SubmitOp(*listener_->loop_, Operation(),
                                                  detail::PrepareCancelAllByFd(listener_->fd_));
    if (!submitted.has_value()) {
      listener_->pending_close_ = nullptr;
      convergence_.SetError(submitted.error());
      return false;
    }

    // SubmitOp() cannot dispatch a CQE reentrantly on this owner thread.
    // Commit listener close only after the cancel SQE has joined this loop's
    // submission protocol; otherwise a local preparation failure must leave
    // an active source unchanged.
    listener_->closed_ = true;
    if (listener_->accept_source_ != nullptr) {
      listener_->accept_source_->OnListenerClosed();
    }

    return true;
  }

  Result<void> await_resume() noexcept {
    ALYRN_CHECK(convergence_.HasResult(), "Uring listener Close resumed before convergence");
    return convergence_.TakeResult();
  }

  void TryComplete(::alyrn::uring::detail::Op* current = nullptr) noexcept {
    if (listener_ == nullptr ||
        convergence_.TryAuthorizeClose(listener_->pending_accepts_ != 0) == false) {
      return;
    }

    Loop* loop = listener_->loop_;
    listener_->pending_close_ = nullptr;
    convergence_.SetResult(CloseFd());
    listener_ = nullptr;
    Operation()->resume_work.SetHandle(convergence_.Continuation());
    if (current != Operation()) {
      detail::LoopAccess::ScheduleCompletion(*loop, &Operation()->resume_work);
    }
  }

private:
  static void OnCancelComplete(::alyrn::uring::detail::Op* op) noexcept {
    auto* self = OpHook::OwnerFrom(op);
    self->convergence_.MarkCancelRequestTerminal();
    self->TryComplete(op);
  }

  Result<void> CloseFd() noexcept {
    const int fd = std::exchange(listener_->fd_, -1);
    if (fd < 0) {
      return Result<void>{};
    }
    if (::close(fd) < 0) {
      return std::unexpected(CurrentErrno());
    }
    return Result<void>{};
  }

  Listener* listener_;
  detail::FdCloseConvergence convergence_;
};

namespace detail {

void DispatchAcceptComplete(::alyrn::uring::detail::Op* op) noexcept {
  Listener::AcceptAwaiter::OnComplete(op);
}

void DispatchListenerCloseComplete(::alyrn::uring::detail::Op* op) noexcept {
  Listener::CloseAwaiter::OnCancelComplete(op);
}

CompletionDisposition DispatchAcceptSourceComplete(::alyrn::uring::detail::Op* op,
                                                   CompletionEvent event) noexcept {
  auto* operation = static_cast<AcceptSource::AcceptOperation*>(op);
  return operation->Source()->OnCompletion(event);
}

void DispatchAcceptSourceCancelComplete(::alyrn::uring::detail::Op* op) noexcept {
  auto* operation = static_cast<AcceptSource::CancelOperation*>(op);

  int result = -EIO;
  if (op->result.HasValue()) {
    result = *op->result;
  }
  operation->Source()->OnCancelComplete(result);
}

}  // namespace detail

Result<Listener> Listener::Create(Loop* loop, const net::Endpoint& listen_addr,
                                  ListenOptions options) noexcept {
  ALYRN_CHECK(loop != nullptr, "Listener requires an owner loop");
  ALYRN_CHECK(loop->IsInLoopThread(), "Listener created from wrong Loop thread");

  auto fd = CreatedListenFd(listen_addr, options);
  if (!fd.has_value()) {
    return std::unexpected(fd.error());
  }
  return Listener(loop, *fd, options.zero_copy_writes, options.tcp_options);
}

Listener::Listener(Loop* loop, int fd, bool zero_copy_writes,
                   net::TcpOptions tcp_options) noexcept
    : loop_(loop),
      fd_(fd),
      zero_copy_writes_(zero_copy_writes),
      tcp_options_(tcp_options) {
  ALYRN_CHECK(loop_ != nullptr, "Listener requires an owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Listener created from wrong Loop thread");
  ALYRN_CHECK(fd_ >= 0, "Listener requires a valid file descriptor");
}

Listener::Listener(Listener&& other) noexcept
    : loop_(PrepareMove(other)),
      fd_(std::exchange(other.fd_, -1)),
      zero_copy_writes_(other.zero_copy_writes_),
      tcp_options_(other.tcp_options_),
      closed_(other.closed_) {
  other.closed_ = true;
}

Listener& Listener::operator=(Listener&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  Loop* other_loop = PrepareMove(other);
  ALYRN_CHECK(loop_ == nullptr || loop_ == other_loop,
                 "Listener move requires both objects to use the same Loop");
  if (loop_ != nullptr) {
    ResetForMove();
  }

  loop_ = other_loop;
  fd_ = std::exchange(other.fd_, -1);
  pending_accepts_ = 0;
  pending_close_ = nullptr;
  accept_source_ = nullptr;
  zero_copy_writes_ = other.zero_copy_writes_;
  tcp_options_ = other.tcp_options_;
  closed_ = other.closed_;
  other.closed_ = true;
  return *this;
}

Listener::~Listener() {
  ALYRN_CHECK(pending_accepts_ == 0, "Listener destroyed with pending accept operations");
  ALYRN_CHECK(pending_close_ == nullptr, "Listener destroyed with a pending close operation");
  ALYRN_CHECK(accept_source_ == nullptr, "Listener destroyed with an active AcceptSource");
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

coro::Task<Result<Stream>> Listener::Accept() { co_return co_await AcceptAwaiter(*this); }

coro::Task<Result<void>> Listener::Close() { co_return co_await CloseAwaiter(*this); }

Result<net::Endpoint> Listener::LocalAddress() const noexcept {
  if (closed_ || fd_ < 0) {
    return std::unexpected(Errno(EBADF));
  }
  return GetLocalAddress(fd_);
}

Result<AcceptSource> Listener::CreateAcceptSource(net::AcceptSourceOptions options) noexcept {
  RequireOwnerLoop();
  if (loop_->State() == backend::LoopState::kStopping ||
      loop_->State() == backend::LoopState::kStopped) {
    return std::unexpected(Errno(ECANCELED));
  }
  if (closed_ || fd_ < 0) {
    return std::unexpected(Errno(EBADF));
  }
  if (pending_accepts_ != 0 || pending_close_ != nullptr || accept_source_ != nullptr) {
    return std::unexpected(Errno(EBUSY));
  }

  auto state = AcceptSourceStateMachine::Create(options);
  if (!state.has_value()) {
    return std::unexpected(state.error());
  }
  return AcceptSource(this, std::move(*state));
}

void Listener::RequireOwnerLoop() const noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Listener operation has no owner loop");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Listener operation called from wrong Loop thread");
}

void Listener::NotifyCloseProgress() noexcept {
  if (pending_close_ != nullptr) {
    pending_close_->TryComplete();
  }
}

void Listener::ResetForMove() noexcept {
  ALYRN_CHECK(loop_ != nullptr, "Listener move destination is not initialized");
  ALYRN_CHECK(loop_->IsInLoopThread(), "Listener move called from wrong Loop thread");
  ALYRN_CHECK(pending_accepts_ == 0, "Listener move destination has pending accept operations");
  ALYRN_CHECK(pending_close_ == nullptr,
                 "Listener move destination has a pending close operation");
  ALYRN_CHECK(accept_source_ == nullptr, "Listener move destination has an active AcceptSource");

  const int fd = std::exchange(fd_, -1);
  if (fd >= 0) {
    ::close(fd);
  }
}

Loop* Listener::PrepareMove(Listener& other) noexcept {
  ALYRN_CHECK(other.loop_ != nullptr, "Listener move source is not initialized");
  ALYRN_CHECK(other.loop_->IsInLoopThread(), "Listener move called from wrong Loop thread");
  ALYRN_CHECK(other.pending_accepts_ == 0,
                 "Listener cannot move with pending accept operations");
  ALYRN_CHECK(other.pending_close_ == nullptr,
                 "Listener cannot move with a pending close operation");
  ALYRN_CHECK(other.accept_source_ == nullptr,
                 "Listener cannot move with an active AcceptSource");

  Loop* loop = other.loop_;
  other.loop_ = nullptr;
  return loop;
}

}  // namespace alyrn::uring
