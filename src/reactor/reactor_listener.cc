// SPDX-License-Identifier: MIT
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <expected>
#include <utility>

#include "coropact/backend/detail/value_result_state.h"
#include "coropact/base/check.h"
#include "coropact/result.h"
#include "coropact/base/try.h"
#include "coropact/net/accept_source.h"
#include "coropact/net/socket.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/operation/detail/single_result_lifecycle.h"
#include "coropact/reactor/detail/loop_access.h"
#include "coropact/reactor/detail/result_state.h"
#include "coropact/reactor/listener.h"

namespace coropact::reactor {

using detail::LoopAccess;

namespace {

bool IsWouldBlock(int err) noexcept { return err == EAGAIN || err == EWOULDBLOCK; }

Error SocketError(int fd) noexcept {
  int err = 0;
  auto len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    return CurrentErrno();
  }
  if (err == 0) {
    err = EIO;
  }
  return Errno(err);
}

EventLoop* CheckLoop(EventLoop* loop) noexcept {
  COROPACT_CHECK(loop != nullptr, "ReactorListener: loop must not be null");
  COROPACT_CHECK(loop->IsInLoopThread(), "ReactorListener created from wrong EventLoop thread");
  return loop;
}

Result<net::Socket> TryCreateListenSocket(const net::Endpoint& listen_addr,
                                                ReactorListenerOptions options) noexcept {
  COROPACT_TRY_VALUE(fd, net::CreateNonBlockingSocket(listen_addr.native_family()));
  net::Socket socket(fd);

  COROPACT_TRY(net::SetReuseAddr(socket.fd(), options.reuse_addr));

  if (options.reuse_port) {
    COROPACT_TRY(net::SetReusePort(socket.fd(), true));
  }

  if (::bind(socket.fd(), listen_addr.sock_addr(), listen_addr.sock_addr_len()) < 0) {
    return std::unexpected(CurrentErrno());
  }

  if (::listen(socket.fd(), SOMAXCONN) < 0) {
    return std::unexpected(CurrentErrno());
  }

  return socket;
}

int CreateListenSocket(sa_family_t family) {
  auto fd = net::CreateNonBlockingSocket(family);
  COROPACT_CHECK(fd.has_value(), "ReactorListener: failed to create listening socket");
  return *fd;
}

}  // namespace

class ReactorListener::AcceptAwaiter {
public:
  explicit AcceptAwaiter(ReactorListener& listener) noexcept : listener_(&listener) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    listener_->RequireOwnerLoop();
    if (listener_->loop_->State() == backend::LoopState::kStopping ||
        listener_->loop_->State() == backend::LoopState::kStopped) {
      CompleteInline(std::unexpected(Errno(ECANCELED)));
      return false;
    }
    if (listener_->pending_accept_ != nullptr) {
      CompleteInline(std::unexpected(Errno(EBUSY)));
      return false;
    }

    continuation_.Bind(continuation);

    Result<ReactorStream> result = TryAccept();
    if (result.has_value() || !IsWouldBlock(result.error().value())) {
      CompleteInline(std::move(result));
      return false;
    }

    listener_->pending_accept_ = this;
    if (!listener_->channel_.IsReading()) {
      listener_->channel_.EnableReading();
    }
    return true;
  }

  Result<ReactorStream> await_resume() noexcept { return result_.Take(); }

private:
  friend class ReactorListener;

  void CompleteInline(Result<ReactorStream> result) noexcept {
    result_.SetResult(std::move(result));
    COROPACT_CHECK(lifecycle_.TryAuthorizeResult(), "Reactor accept result was authorized twice");
    COROPACT_CHECK(lifecycle_.TryAuthorizeRelease(),
                   "Reactor accept release was not authorized after its result");
  }

  [[nodiscard]] bool CompleteResult(Result<ReactorStream> result) noexcept {
    if (!lifecycle_.TryAuthorizeResult()) {
      return false;
    }
    result_.SetResult(std::move(result));
    return true;
  }

  [[nodiscard]] bool TryAuthorizeRelease() noexcept { return lifecycle_.TryAuthorizeRelease(); }

  [[nodiscard]] bool TryAuthorizeContinuation() noexcept {
    return lifecycle_.TryAuthorizeContinuation();
  }

  void ScheduleContinuation() noexcept { continuation_.Schedule(); }

public:
  void OnReady() noexcept {
    Result<ReactorStream> result = TryAccept();
    if (!result.has_value() && IsWouldBlock(result.error().value())) {
      return;
    }
    listener_->CompleteAccept(std::move(result));
  }

private:
  Result<ReactorStream> TryAccept() noexcept {
    int fd = -1;
    net::Endpoint peer_addr(0);
    do {
      fd = listener_->socket_.Accept(&peer_addr);
    } while (fd < 0 && errno == EINTR);

    if (fd < 0) {
      return std::unexpected(CurrentErrno());
    }
    return ReactorStream(listener_->loop_, fd, peer_addr, listener_->stream_options_);
  }

  ReactorListener* listener_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::SingleResultLifecycle lifecycle_;
  backend::detail::ValueResultState<ReactorStream> result_;
};

bool ReactorAcceptSource::NextAwaiter::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
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

  if (source_->state_.State() == net::detail::AcceptSourceState::kIdle) {
    if (source_->listener_->loop_->State() == backend::LoopState::kStopping ||
        source_->listener_->loop_->State() == backend::LoopState::kStopped) {
      result_.SetError(Errno(ECANCELED));
      (void)(completion_gate_.TryComplete());
      return false;
    }
    if (source_->listener_->closed_) {
      source_->state_.RequestStop();
      result_.SetResult(NextResult(std::in_place, Event{}));
      (void)(completion_gate_.TryComplete());
      return false;
    }
    if (source_->listener_->pending_accept_ != nullptr ||
        (source_->listener_->accept_source_ != nullptr &&
         source_->listener_->accept_source_ != source_)) {
      result_.SetError(Errno(EBUSY));
      (void)(completion_gate_.TryComplete());
      return false;
    }
    auto started = source_->state_.Start();
    if (!started.has_value()) {
      result_.SetError(started.error());
      (void)(completion_gate_.TryComplete());
      return false;
    }
    source_->listener_->accept_source_ = source_;
  }

  NextResult result;
  if (source_->TryTakeNext(result)) {
    result_.SetResult(std::move(result));
    (void)(completion_gate_.TryComplete());
    return false;
  }

  continuation_.Bind(continuation);
  source_->pending_next_ = this;
  source_->EnsureAdmission();

  // Admission may complete synchronously in a readiness backend. Recheck
  // after arming so a just-available event does not leave the coroutine
  // parked until a second poll notification.
  if (source_->TryTakeNext(result)) {
    source_->pending_next_ = nullptr;
    result_.SetResult(std::move(result));
    (void)(completion_gate_.TryComplete());
    return false;
  }
  return true;
}

ReactorAcceptSource::NextResult ReactorAcceptSource::NextAwaiter::await_resume() noexcept {
  return result_.Take();
}

void ReactorAcceptSource::NextAwaiter::Complete(NextResult result) noexcept {
  if (!completion_gate_.TryComplete()) {
    return;
  }
  result_.SetResult(std::move(result));
  continuation_.Schedule();
}

ReactorAcceptSource::ReactorAcceptSource(ReactorListener* listener,
                                         net::detail::AcceptSourceStateMachine state) noexcept
    : listener_(listener), state_(std::move(state)) {}

ReactorAcceptSource::ReactorAcceptSource(ReactorAcceptSource&& other) noexcept
    : listener_(std::exchange(other.listener_, nullptr)),
      state_(std::move(other.state_)),
      events_(std::move(other.events_)),
      terminal_error_(std::move(other.terminal_error_)),
      pending_next_(nullptr) {
  COROPACT_CHECK(other.pending_next_ == nullptr,
                 "ReactorAcceptSource cannot move with a pending Next");
  COROPACT_CHECK(state_.State() != net::detail::AcceptSourceState::kActive &&
                     state_.State() != net::detail::AcceptSourceState::kStopping,
                 "ReactorAcceptSource cannot move while it is running");
  if (listener_ != nullptr && listener_->accept_source_ == &other) {
    listener_->accept_source_ = this;
  }
}

ReactorAcceptSource& ReactorAcceptSource::operator=(ReactorAcceptSource&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  COROPACT_CHECK(pending_next_ == nullptr, "ReactorAcceptSource destination has a pending Next");
  COROPACT_CHECK(state_.State() != net::detail::AcceptSourceState::kActive &&
                     state_.State() != net::detail::AcceptSourceState::kStopping,
                 "ReactorAcceptSource destination is running");
  COROPACT_CHECK(other.pending_next_ == nullptr,
                 "ReactorAcceptSource cannot move with a pending Next");
  COROPACT_CHECK(other.state_.State() != net::detail::AcceptSourceState::kActive &&
                     other.state_.State() != net::detail::AcceptSourceState::kStopping,
                 "ReactorAcceptSource cannot move while it is running");

  if (listener_ != nullptr && listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }

  listener_ = std::exchange(other.listener_, nullptr);
  state_ = std::move(other.state_);
  events_ = std::move(other.events_);
  terminal_error_ = std::move(other.terminal_error_);
  if (listener_ != nullptr && listener_->accept_source_ == &other) {
    listener_->accept_source_ = this;
  }
  return *this;
}

ReactorAcceptSource::~ReactorAcceptSource() {
  if (listener_ == nullptr) {
    return;
  }
  COROPACT_CHECK(listener_->loop_->IsInLoopThread(),
                 "ReactorAcceptSource destructor called from wrong thread");
  COROPACT_CHECK(pending_next_ == nullptr, "ReactorAcceptSource destroyed with a pending Next");
  const auto state = state_.State();
  COROPACT_CHECK(state == net::detail::AcceptSourceState::kIdle ||
                     state == net::detail::AcceptSourceState::kDraining ||
                     state == net::detail::AcceptSourceState::kTerminal,
                 "ReactorAcceptSource destroyed before reaching a safe lifecycle state");
  COROPACT_CHECK(state_.ArmedRequests() == 0, "ReactorAcceptSource destroyed with an armed accept");
  if (listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }
}

coro::Task<Result<void>> ReactorAcceptSource::Stop() {
  if (listener_ == nullptr) {
    co_return Result<void>{};
  }
  if (!listener_->loop_->IsInLoopThread()) {
    co_return std::unexpected(Errno(EINVAL));
  }

  if (state_.State() == net::detail::AcceptSourceState::kIdle) {
    state_.RequestStop();
    ReleaseListenerReservation();
    co_return Result<void>{};
  }

  if (state_.State() == net::detail::AcceptSourceState::kActive ||
      state_.State() == net::detail::AcceptSourceState::kPausing ||
      state_.State() == net::detail::AcceptSourceState::kPaused ||
      state_.State() == net::detail::AcceptSourceState::kStopping) {
    if (listener_->channel_.IsReading()) {
      listener_->channel_.DisableReading();
    }
    if (state_.ArmedRequests() != 0) {
      auto completed = state_.CompleteRequest(false);
      if (!completed.has_value()) {
        co_return std::unexpected(completed.error());
      }
    }
    state_.RequestStop();
  }

  DeliverNextIfReady();
  ReleaseListenerReservation();
  co_return Result<void>{};
}

void ReactorAcceptSource::OnReady() noexcept {
  if (state_.State() != net::detail::AcceptSourceState::kActive) {
    return;
  }

  if (state_.ArmedRequests() == 0 && !state_.TryArm()) {
    if (listener_->channel_.IsReading()) {
      listener_->channel_.DisableReading();
    }
    return;
  }

  while (state_.State() == net::detail::AcceptSourceState::kActive && state_.ArmedRequests() != 0) {
    Result<ReactorStream> accepted = TryAccept();
    if (!accepted.has_value()) {
      Error error = accepted.error();
      auto completed = state_.CompleteRequest(false);
      COROPACT_CHECK(completed.has_value(),
                     "ReactorAcceptSource: failed to record accept completion");
      if (IsWouldBlock(error.value())) {
        break;
      }
      Fail(error);
      return;
    }

    try {
      events_.push_back(std::move(*accepted));
    } catch (...) {
      auto completed = state_.CompleteRequest(false);
      COROPACT_CHECK(completed.has_value(),
                     "ReactorAcceptSource: failed to record accept completion");
      Fail(Errno(ENOMEM));
      return;
    }

    auto completed = state_.CompleteRequest(true);
    COROPACT_CHECK(completed.has_value(), "ReactorAcceptSource: failed to record accepted stream");
    if (!state_.TryArm()) {
      if (state_.QueuedEvents() >= state_.Options().event_capacity) {
        auto paused = state_.RequestPause();
        COROPACT_CHECK(paused.has_value(), "ReactorAcceptSource: failed to enter the paused state");
      }
      break;
    }
  }

  EnsureAdmission();
  DeliverNextIfReady();
}

void ReactorAcceptSource::OnError(Error error) noexcept {
  if (state_.State() != net::detail::AcceptSourceState::kActive) {
    return;
  }
  if (state_.ArmedRequests() != 0) {
    auto completed = state_.CompleteRequest(false);
    COROPACT_CHECK(completed.has_value(), "ReactorAcceptSource: failed to record error completion");
  }
  Fail(error);
}

void ReactorAcceptSource::OnListenerClosed() noexcept {
  if (listener_->channel_.IsReading()) {
    listener_->channel_.DisableReading();
  }
  if (state_.ArmedRequests() != 0) {
    auto completed = state_.CompleteRequest(false);
    COROPACT_CHECK(completed.has_value(), "ReactorAcceptSource: failed to drain close completion");
  }
  state_.RequestStop();
  DeliverNextIfReady();
}

void ReactorAcceptSource::EnsureAdmission() noexcept {
  if (listener_ == nullptr || listener_->closed_) {
    return;
  }
  if (state_.State() != net::detail::AcceptSourceState::kActive) {
    if (listener_->channel_.IsReading()) {
      listener_->channel_.DisableReading();
    }
    return;
  }
  if (state_.ArmedRequests() == 0 && state_.TryArm()) {
    listener_->channel_.EnableReading();
  }
  if (state_.ArmedRequests() == 0 && !state_.CanArm() && listener_->channel_.IsReading()) {
    listener_->channel_.DisableReading();
  }
}

void ReactorAcceptSource::DeliverNextIfReady() noexcept {
  if (pending_next_ == nullptr) {
    ReleaseListenerReservation();
    return;
  }

  NextResult result;
  if (!TryTakeNext(result)) {
    return;
  }

  NextAwaiter* awaiter = std::exchange(pending_next_, nullptr);
  awaiter->Complete(std::move(result));
}

bool ReactorAcceptSource::TryTakeNext(NextResult& result) noexcept {
  if (!events_.empty()) {
    Event event(std::in_place, std::move(events_.front()));
    events_.pop_front();
    COROPACT_CHECK(state_.ConsumeEvent(),
                   "ReactorAcceptSource: queue and state became inconsistent");
    result = NextResult(std::in_place, std::move(event));
    if (state_.State() == net::detail::AcceptSourceState::kPaused) {
      (void)(state_.TryResume());
    }
    EnsureAdmission();
    return true;
  }

  if (state_.State() == net::detail::AcceptSourceState::kTerminal) {
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

void ReactorAcceptSource::ReleaseListenerReservation() noexcept {
  if (listener_ != nullptr && state_.State() == net::detail::AcceptSourceState::kTerminal &&
      listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }
}

void ReactorAcceptSource::Fail(Error error) noexcept {
  if (!terminal_error_.has_value()) {
    terminal_error_ = error;
  }
  if (listener_->channel_.IsReading()) {
    listener_->channel_.DisableReading();
  }
  state_.RequestStop();
  DeliverNextIfReady();
}

Result<ReactorStream> ReactorAcceptSource::TryAccept() noexcept {
  int fd = -1;
  net::Endpoint peer_addr(0);
  do {
    fd = listener_->socket_.Accept(&peer_addr);
  } while (fd < 0 && errno == EINTR);

  if (fd < 0) {
    return std::unexpected(CurrentErrno());
  }
  return ReactorStream(listener_->loop_, fd, peer_addr, listener_->stream_options_);
}

ReactorListener::ReactorListener(EventLoop* loop, const net::Endpoint& listen_addr,
                                 ReactorListenerOptions options)
    : loop_(CheckLoop(loop)),
      socket_(CreateListenSocket(listen_addr.native_family())),
      channel_(loop_, socket_.fd()),
      stream_options_(options.stream_options) {
  socket_.SetReuseAddr(options.reuse_addr);
  if (options.reuse_port) {
    socket_.SetReusePort(true);
  }
  socket_.BindAddress(listen_addr);
  socket_.Listen();

  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

ReactorListener::ReactorListener(EventLoop* loop, net::Socket socket,
                                 ReactorStreamOptions stream_options) noexcept
    : loop_(CheckLoop(loop)),
      socket_(std::move(socket)),
      channel_(loop_, socket_.fd()),
      stream_options_(stream_options) {
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

Result<ReactorListener> ReactorListener::Create(EventLoop* loop,
                                                      const net::Endpoint& listen_addr,
                                                      ReactorListenerOptions options) noexcept {
  if (loop == nullptr) {
    return std::unexpected(Errno(EINVAL));
  }

  COROPACT_TRY_VALUE(socket, TryCreateListenSocket(listen_addr, options));
  return ReactorListener(loop, std::move(socket), options.stream_options);
}

ReactorListener::ReactorListener(ReactorListener&& other) noexcept
    : loop_(PrepareMove(other)),
      socket_(std::move(other.socket_)),
      channel_(std::move(other.channel_)),
      stream_options_(other.stream_options_),
      pending_accept_(nullptr),
      accept_source_(nullptr),
      closed_(other.closed_) {
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
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
  stream_options_ = other.stream_options_;
  pending_accept_ = nullptr;
  accept_source_ = nullptr;
  closed_ = other.closed_;
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
  other.closed_ = true;
  return *this;
}

ReactorListener::~ReactorListener() {
  if (loop_ == nullptr) {
    return;
  }
  RequireOwnerLoop();
  COROPACT_CHECK(pending_accept_ == nullptr, "ReactorListener destroyed with a pending accept");
  COROPACT_CHECK(accept_source_ == nullptr,
                 "ReactorListener destroyed with an active AcceptSource");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
}

coro::Task<Result<ReactorStream>> ReactorListener::Accept() {
  RequireOwnerLoop();
  if (closed_) {
    co_return std::unexpected(Errno(EBADF));
  }
  if (accept_source_ != nullptr) {
    co_return std::unexpected(Errno(EBUSY));
  }
  co_return co_await AcceptAwaiter(*this);
}

Result<ReactorAcceptSource> ReactorListener::AcceptSource(
    net::AcceptSourceOptions options) noexcept {
  RequireOwnerLoop();
  if (loop_->State() == backend::LoopState::kStopping ||
      loop_->State() == backend::LoopState::kStopped) {
    return std::unexpected(Errno(ECANCELED));
  }
  if (closed_) {
    return std::unexpected(Errno(EBADF));
  }
  if (pending_accept_ != nullptr || accept_source_ != nullptr) {
    return std::unexpected(Errno(EBUSY));
  }
  COROPACT_TRY_VALUE(state, net::detail::AcceptSourceStateMachine::Create(options));
  return ReactorAcceptSource(this, std::move(state));
}

coro::Task<Result<void>> ReactorListener::Close() {
  RequireOwnerLoop();
  CloseNow();
  co_return Result<void>{};
}

void ReactorListener::CloseNow() noexcept {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorListener::CloseNow called from wrong thread");
  if (closed_) {
    return;
  }

  closed_ = true;
  if (pending_accept_ != nullptr) {
    CompleteAccept(std::unexpected(Errno(ECANCELED)));
  }
  if (accept_source_ != nullptr) {
    accept_source_->OnListenerClosed();
  }
  DetachChannel();
  socket_.Close();
}

Result<net::Endpoint> ReactorListener::LocalAddress() const {
  RequireOwnerLoop();
  if (closed_) {
    return std::unexpected(Errno(EBADF));
  }
  return socket_.LocalEndpoint();
}

void ReactorListener::HandleRead() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorListener::HandleRead called from wrong thread");
  if (pending_accept_ != nullptr) {
    pending_accept_->OnReady();
  } else if (accept_source_ != nullptr) {
    accept_source_->OnReady();
  }
}

void ReactorListener::HandleError() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorListener::HandleError called from wrong thread");
  if (pending_accept_ != nullptr) {
    CompleteAccept(std::unexpected(SocketError(socket_.fd())));
  } else if (accept_source_ != nullptr) {
    accept_source_->OnError(SocketError(socket_.fd()));
  }
}

void ReactorListener::DispatchRead(void* context) noexcept {
  static_cast<ReactorListener*>(context)->HandleRead();
}

void ReactorListener::DispatchError(void* context) noexcept {
  static_cast<ReactorListener*>(context)->HandleError();
}

void ReactorListener::CompleteAccept(Result<ReactorStream> result) {
  COROPACT_DCHECK(loop_->IsInLoopThread(),
                  "ReactorListener::CompleteAccept called from wrong thread");
  AcceptAwaiter* awaiter = pending_accept_;
  if (awaiter == nullptr) {
    return;
  }
  COROPACT_CHECK(awaiter->CompleteResult(std::move(result)),
                 "ReactorListener::CompleteAccept result was already authorized");
  COROPACT_CHECK(awaiter->TryAuthorizeRelease(),
                 "ReactorListener::CompleteAccept release was not authorized after its result");

  AcceptAwaiter* released = std::exchange(pending_accept_, nullptr);
  COROPACT_CHECK(released == awaiter,
                 "ReactorListener::CompleteAccept pending slot changed during completion");
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  COROPACT_CHECK(awaiter->TryAuthorizeContinuation(),
                 "ReactorListener::CompleteAccept continuation was not authorized after release");
  awaiter->ScheduleContinuation();
}

void ReactorListener::DetachChannel() {
  COROPACT_DCHECK(loop_->IsInLoopThread(),
                  "ReactorListener::DetachChannel called from wrong thread");
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (channel_.IsRegistered()) {
    channel_.Remove();
  }
}

void ReactorListener::RequireOwnerLoop() const noexcept {
  COROPACT_CHECK(loop_ != nullptr, "ReactorListener operation has no owner EventLoop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "ReactorListener operation called from wrong EventLoop thread");
}

void ReactorListener::BindChannelCallbacks() noexcept {
  try {
    channel_.SetReadCallback(&ReactorListener::DispatchRead, this);
    channel_.SetErrorCallback(&ReactorListener::DispatchError, this);
  } catch (...) {
    COROPACT_CHECK(false, "ReactorListener: failed to bind channel callbacks");
  }
}

void ReactorListener::ResetForMove() noexcept {
  COROPACT_CHECK(loop_ != nullptr, "ReactorListener move destination is not initialized");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "ReactorListener move called from wrong EventLoop thread");
  COROPACT_CHECK(pending_accept_ == nullptr,
                 "ReactorListener move destination has a pending accept");
  COROPACT_CHECK(accept_source_ == nullptr,
                 "ReactorListener move destination has an active AcceptSource");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
  socket_.Close();
}

EventLoop* ReactorListener::PrepareMove(ReactorListener& other) noexcept {
  COROPACT_CHECK(other.loop_ != nullptr, "ReactorListener move source is not initialized");
  COROPACT_CHECK(other.loop_->IsInLoopThread(),
                 "ReactorListener move called from wrong EventLoop thread");
  COROPACT_CHECK(other.pending_accept_ == nullptr,
                 "ReactorListener cannot move with a pending accept operation");
  COROPACT_CHECK(other.accept_source_ == nullptr,
                 "ReactorListener cannot move with an active AcceptSource");

  other.DetachChannel();
  LoopAccess::UnregisterShutdownParticipant(*other.loop_, other.shutdown_participant_);
  EventLoop* loop = other.loop_;
  other.loop_ = nullptr;
  return loop;
}

void ReactorListener::DispatchLoopStop(void* context) noexcept {
  static_cast<ReactorListener*>(context)->CloseNow();
}

}  // namespace coropact::reactor
