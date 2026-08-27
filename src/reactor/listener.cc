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

Loop* CheckLoop(Loop* loop) noexcept {
  COROPACT_CHECK(loop != nullptr, "Listener: loop must not be null");
  COROPACT_CHECK(loop->IsInLoopThread(), "Listener created from wrong Loop thread");
  return loop;
}

Result<net::Socket> TryCreateListenSocket(const net::Endpoint& listen_addr,
                                                ListenerOptions options) noexcept {
  auto fd = net::CreateNonBlockingSocket(listen_addr.NativeFamily());
  if (!fd.has_value()) {
    return std::unexpected(fd.error());
  }
  net::Socket socket(*fd);

  auto reuse_addr = net::SetReuseAddr(socket.fd(), options.reuse_addr);
  if (!reuse_addr.has_value()) {
    return std::unexpected(reuse_addr.error());
  }

  if (options.reuse_port) {
    auto reuse_port = net::SetReusePort(socket.fd(), true);
    if (!reuse_port.has_value()) {
      return std::unexpected(reuse_port.error());
    }
  }

  if (::bind(socket.fd(), listen_addr.SockAddr(), listen_addr.SockAddrLen()) < 0) {
    return std::unexpected(CurrentErrno());
  }

  if (::listen(socket.fd(), SOMAXCONN) < 0) {
    return std::unexpected(CurrentErrno());
  }

  return socket;
}

int CreateListenSocket(sa_family_t family) {
  auto fd = net::CreateNonBlockingSocket(family);
  COROPACT_CHECK(fd.has_value(), "Listener: failed to create listening socket");
  return *fd;
}

}  // namespace

class Listener::AcceptAwaiter {
public:
  explicit AcceptAwaiter(Listener& listener) noexcept : listener_(&listener) {}

  bool await_ready() const noexcept { return false; }

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

    Result<Stream> result = TryAccept();
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

  Result<Stream> await_resume() noexcept { return result_.Take(); }

private:
  friend class Listener;

  void CompleteInline(Result<Stream> result) noexcept {
    result_.SetResult(std::move(result));
    COROPACT_CHECK(lifecycle_.TryAuthorizeResult(), "Reactor accept result was authorized twice");
    COROPACT_CHECK(lifecycle_.TryAuthorizeRelease(),
                   "Reactor accept release was not authorized after its result");
  }

  [[nodiscard]] bool CompleteResult(Result<Stream> result) noexcept {
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
    Result<Stream> result = TryAccept();
    if (!result.has_value() && IsWouldBlock(result.error().value())) {
      return;
    }
    listener_->CompleteAccept(std::move(result));
  }

private:
  Result<Stream> TryAccept() noexcept {
    int fd = -1;
    net::Endpoint peer_addr(0);
    do {
      fd = listener_->socket_.Accept(&peer_addr);
    } while (fd < 0 && errno == EINTR);

    if (fd < 0) {
      return std::unexpected(CurrentErrno());
    }

    auto configured = net::ApplyTcpOptions(fd, listener_->tcp_options_);
    if (!configured.has_value()) {
      (void)::close(fd);
      return std::unexpected(configured.error());
    }
    return Stream(listener_->loop_, fd, peer_addr, listener_->stream_options_);
  }

  Listener* listener_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::SingleResultLifecycle lifecycle_;
  backend::detail::ValueResultState<Stream> result_;
};

bool AcceptSource::NextAwaiter::await_suspend(
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

AcceptSource::NextResult AcceptSource::NextAwaiter::await_resume() noexcept {
  return result_.Take();
}

void AcceptSource::NextAwaiter::Complete(NextResult result) noexcept {
  if (!completion_gate_.TryComplete()) {
    return;
  }
  result_.SetResult(std::move(result));
  continuation_.Schedule();
}

AcceptSource::AcceptSource(Listener* listener,
                                         net::detail::AcceptSourceStateMachine state) noexcept
    : listener_(listener), state_(std::move(state)) {}

AcceptSource::AcceptSource(AcceptSource&& other) noexcept
    : listener_(std::exchange(other.listener_, nullptr)),
      state_(std::move(other.state_)),
      events_(std::move(other.events_)),
      terminal_error_(std::move(other.terminal_error_)),
      pending_next_(nullptr) {
  COROPACT_CHECK(other.pending_next_ == nullptr,
                 "AcceptSource cannot move with a pending Next");
  COROPACT_CHECK(state_.State() != net::detail::AcceptSourceState::kActive &&
                     state_.State() != net::detail::AcceptSourceState::kStopping,
                 "AcceptSource cannot move while it is running");
  if (listener_ != nullptr && listener_->accept_source_ == &other) {
    listener_->accept_source_ = this;
  }
}

AcceptSource& AcceptSource::operator=(AcceptSource&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  COROPACT_CHECK(pending_next_ == nullptr, "AcceptSource destination has a pending Next");
  COROPACT_CHECK(state_.State() != net::detail::AcceptSourceState::kActive &&
                     state_.State() != net::detail::AcceptSourceState::kStopping,
                 "AcceptSource destination is running");
  COROPACT_CHECK(other.pending_next_ == nullptr,
                 "AcceptSource cannot move with a pending Next");
  COROPACT_CHECK(other.state_.State() != net::detail::AcceptSourceState::kActive &&
                     other.state_.State() != net::detail::AcceptSourceState::kStopping,
                 "AcceptSource cannot move while it is running");

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

AcceptSource::~AcceptSource() {
  if (listener_ == nullptr) {
    return;
  }
  COROPACT_CHECK(listener_->loop_->IsInLoopThread(),
                 "AcceptSource destructor called from wrong thread");
  COROPACT_CHECK(pending_next_ == nullptr, "AcceptSource destroyed with a pending Next");
  const auto state = state_.State();
  COROPACT_CHECK(state == net::detail::AcceptSourceState::kIdle ||
                     state == net::detail::AcceptSourceState::kDraining ||
                     state == net::detail::AcceptSourceState::kTerminal,
                 "AcceptSource destroyed before reaching a safe lifecycle state");
  COROPACT_CHECK(state_.ArmedRequests() == 0, "AcceptSource destroyed with an armed accept");
  if (listener_->accept_source_ == this) {
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

void AcceptSource::OnReady() noexcept {
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
    Result<Stream> accepted = TryAccept();
    if (!accepted.has_value()) {
      Error error = accepted.error();
      auto completed = state_.CompleteRequest(false);
      COROPACT_CHECK(completed.has_value(),
                     "AcceptSource: failed to record accept completion");
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
                     "AcceptSource: failed to record accept completion");
      Fail(Errno(ENOMEM));
      return;
    }

    auto completed = state_.CompleteRequest(true);
    COROPACT_CHECK(completed.has_value(), "AcceptSource: failed to record accepted stream");
    if (!state_.TryArm()) {
      if (state_.QueuedEvents() >= state_.Options().event_capacity) {
        auto paused = state_.RequestPause();
        COROPACT_CHECK(paused.has_value(), "AcceptSource: failed to enter the paused state");
      }
      break;
    }
  }

  EnsureAdmission();
  DeliverNextIfReady();
}

void AcceptSource::OnError(Error error) noexcept {
  if (state_.State() != net::detail::AcceptSourceState::kActive) {
    return;
  }
  if (state_.ArmedRequests() != 0) {
    auto completed = state_.CompleteRequest(false);
    COROPACT_CHECK(completed.has_value(), "AcceptSource: failed to record error completion");
  }
  Fail(error);
}

void AcceptSource::OnListenerClosed() noexcept {
  if (listener_->channel_.IsReading()) {
    listener_->channel_.DisableReading();
  }
  if (state_.ArmedRequests() != 0) {
    auto completed = state_.CompleteRequest(false);
    COROPACT_CHECK(completed.has_value(), "AcceptSource: failed to drain close completion");
  }
  state_.RequestStop();
  DeliverNextIfReady();
}

void AcceptSource::EnsureAdmission() noexcept {
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

void AcceptSource::DeliverNextIfReady() noexcept {
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

bool AcceptSource::TryTakeNext(NextResult& result) noexcept {
  if (!events_.empty()) {
    Event event(std::in_place, std::move(events_.front()));
    events_.pop_front();
    COROPACT_CHECK(state_.ConsumeEvent(),
                   "AcceptSource: queue and state became inconsistent");
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

void AcceptSource::ReleaseListenerReservation() noexcept {
  if (listener_ != nullptr && state_.State() == net::detail::AcceptSourceState::kTerminal &&
      listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }
}

void AcceptSource::Fail(Error error) noexcept {
  if (!terminal_error_.has_value()) {
    terminal_error_ = error;
  }
  if (listener_->channel_.IsReading()) {
    listener_->channel_.DisableReading();
  }
  state_.RequestStop();
  DeliverNextIfReady();
}

Result<Stream> AcceptSource::TryAccept() noexcept {
  int fd = -1;
  net::Endpoint peer_addr(0);
  do {
    fd = listener_->socket_.Accept(&peer_addr);
  } while (fd < 0 && errno == EINTR);

  if (fd < 0) {
    return std::unexpected(CurrentErrno());
  }

  auto configured = net::ApplyTcpOptions(fd, listener_->tcp_options_);
  if (!configured.has_value()) {
    (void)::close(fd);
    return std::unexpected(configured.error());
  }
  return Stream(listener_->loop_, fd, peer_addr, listener_->stream_options_);
}

Listener::Listener(Loop* loop, const net::Endpoint& listen_addr,
                                 ListenerOptions options)
    : loop_(CheckLoop(loop)),
      socket_(CreateListenSocket(listen_addr.NativeFamily())),
      channel_(loop_, socket_.fd()),
      stream_options_(options.stream_options),
      tcp_options_(options.tcp_options) {
  socket_.SetReuseAddr(options.reuse_addr);
  if (options.reuse_port) {
    socket_.SetReusePort(true);
  }
  socket_.BindAddress(listen_addr);
  socket_.Listen();

  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

Listener::Listener(Loop* loop, net::Socket socket, StreamOptions stream_options,
                   net::TcpOptions tcp_options) noexcept
    : loop_(CheckLoop(loop)),
      socket_(std::move(socket)),
      channel_(loop_, socket_.fd()),
      stream_options_(stream_options),
      tcp_options_(tcp_options) {
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

Result<Listener> Listener::Create(Loop* loop,
                                                      const net::Endpoint& listen_addr,
                                                      ListenerOptions options) noexcept {
  if (loop == nullptr) {
    return std::unexpected(Errno(EINVAL));
  }

  auto socket = TryCreateListenSocket(listen_addr, options);
  if (!socket.has_value()) {
    return std::unexpected(socket.error());
  }
  return Listener(loop, std::move(*socket), options.stream_options, options.tcp_options);
}

Listener::Listener(Listener&& other) noexcept
    : loop_(PrepareMove(other)),
      socket_(std::move(other.socket_)),
      channel_(std::move(other.channel_)),
      stream_options_(other.stream_options_),
      tcp_options_(other.tcp_options_),
      pending_accept_(nullptr),
      accept_source_(nullptr),
      closed_(other.closed_) {
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
  other.closed_ = true;
}

Listener& Listener::operator=(Listener&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  Loop* other_loop = PrepareMove(other);
  COROPACT_CHECK(loop_ == nullptr || loop_ == other_loop,
                 "Listener move requires both objects to use the same Loop");
  if (loop_ != nullptr) {
    ResetForMove();
  }

  loop_ = other_loop;
  socket_ = std::move(other.socket_);
  channel_ = std::move(other.channel_);
  stream_options_ = other.stream_options_;
  tcp_options_ = other.tcp_options_;
  pending_accept_ = nullptr;
  accept_source_ = nullptr;
  closed_ = other.closed_;
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
  other.closed_ = true;
  return *this;
}

Listener::~Listener() {
  if (loop_ == nullptr) {
    return;
  }
  RequireOwnerLoop();
  COROPACT_CHECK(pending_accept_ == nullptr, "Listener destroyed with a pending accept");
  COROPACT_CHECK(accept_source_ == nullptr,
                 "Listener destroyed with an active AcceptSource");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
}

coro::Task<Result<Stream>> Listener::Accept() {
  RequireOwnerLoop();
  if (closed_) {
    co_return std::unexpected(Errno(EBADF));
  }
  if (accept_source_ != nullptr) {
    co_return std::unexpected(Errno(EBUSY));
  }
  co_return co_await AcceptAwaiter(*this);
}

Result<AcceptSource> Listener::CreateAcceptSource(
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
  auto state = net::detail::AcceptSourceStateMachine::Create(options);
  if (!state.has_value()) {
    return std::unexpected(state.error());
  }
  return AcceptSource(this, std::move(*state));
}

coro::Task<Result<void>> Listener::Close() {
  RequireOwnerLoop();
  CloseNow();
  co_return Result<void>{};
}

void Listener::CloseNow() noexcept {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "Listener::CloseNow called from wrong thread");
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

Result<net::Endpoint> Listener::LocalAddress() const {
  RequireOwnerLoop();
  if (closed_) {
    return std::unexpected(Errno(EBADF));
  }
  return socket_.LocalEndpoint();
}

void Listener::HandleRead() {
  if (pending_accept_ != nullptr) {
    pending_accept_->OnReady();
  } else if (accept_source_ != nullptr) {
    accept_source_->OnReady();
  }
}

void Listener::HandleError() {
  if (pending_accept_ != nullptr) {
    CompleteAccept(std::unexpected(SocketError(socket_.fd())));
  } else if (accept_source_ != nullptr) {
    accept_source_->OnError(SocketError(socket_.fd()));
  }
}

void Listener::DispatchRead(void* context) noexcept {
  static_cast<Listener*>(context)->HandleRead();
}

void Listener::DispatchError(void* context) noexcept {
  static_cast<Listener*>(context)->HandleError();
}

void Listener::CompleteAccept(Result<Stream> result) {
  AcceptAwaiter* awaiter = pending_accept_;
  if (awaiter == nullptr) {
    return;
  }
  COROPACT_CHECK(awaiter->CompleteResult(std::move(result)),
                 "Listener::CompleteAccept result was already authorized");
  COROPACT_CHECK(awaiter->TryAuthorizeRelease(),
                 "Listener::CompleteAccept release was not authorized after its result");

  AcceptAwaiter* released = std::exchange(pending_accept_, nullptr);
  COROPACT_CHECK(released == awaiter,
                 "Listener::CompleteAccept pending slot changed during completion");
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  COROPACT_CHECK(awaiter->TryAuthorizeContinuation(),
                 "Listener::CompleteAccept continuation was not authorized after release");
  awaiter->ScheduleContinuation();
}

void Listener::DetachChannel() {
  COROPACT_DCHECK(loop_->IsInLoopThread(),
                  "Listener::DetachChannel called from wrong thread");
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (channel_.IsRegistered()) {
    channel_.Remove();
  }
}

void Listener::RequireOwnerLoop() const noexcept {
  COROPACT_CHECK(loop_ != nullptr, "Listener operation has no owner Loop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "Listener operation called from wrong Loop thread");
}

void Listener::BindChannelCallbacks() noexcept {
  try {
    channel_.SetReadCallback(&Listener::DispatchRead, this);
    channel_.SetErrorCallback(&Listener::DispatchError, this);
  } catch (...) {
    COROPACT_CHECK(false, "Listener: failed to bind channel callbacks");
  }
}

void Listener::ResetForMove() noexcept {
  COROPACT_CHECK(loop_ != nullptr, "Listener move destination is not initialized");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "Listener move called from wrong Loop thread");
  COROPACT_CHECK(pending_accept_ == nullptr,
                 "Listener move destination has a pending accept");
  COROPACT_CHECK(accept_source_ == nullptr,
                 "Listener move destination has an active AcceptSource");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
  socket_.Close();
}

Loop* Listener::PrepareMove(Listener& other) noexcept {
  COROPACT_CHECK(other.loop_ != nullptr, "Listener move source is not initialized");
  COROPACT_CHECK(other.loop_->IsInLoopThread(),
                 "Listener move called from wrong Loop thread");
  COROPACT_CHECK(other.pending_accept_ == nullptr,
                 "Listener cannot move with a pending accept operation");
  COROPACT_CHECK(other.accept_source_ == nullptr,
                 "Listener cannot move with an active AcceptSource");

  other.DetachChannel();
  LoopAccess::UnregisterShutdownParticipant(*other.loop_, other.shutdown_participant_);
  Loop* loop = other.loop_;
  other.loop_ = nullptr;
  return loop;
}

void Listener::DispatchLoopStop(void* context) noexcept {
  static_cast<Listener*>(context)->CloseNow();
}

}  // namespace coropact::reactor
