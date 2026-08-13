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
#include "coropact/kqueue/detail/loop_access.h"
#include "coropact/kqueue/detail/result_state.h"
#include "coropact/kqueue/listener.h"
#include "coropact/kqueue/options.h"

namespace coropact::kqueue {

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

KqueueLoop* CheckLoop(KqueueLoop* loop) noexcept {
  COROPACT_CHECK(loop != nullptr, "KqueueListener: loop must not be null");
  COROPACT_CHECK(loop->IsInLoopThread(), "KqueueListener created from wrong KqueueLoop thread");
  return loop;
}

Result<net::Socket> TryCreateListenSocket(const net::Endpoint& listen_addr,
                                                KqueueListenerOptions options) noexcept {
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
  COROPACT_CHECK(fd.has_value(), "KqueueListener: failed to create listening socket");
  return *fd;
}

}  // namespace

class KqueueListener::AcceptAwaiter {
public:
  explicit AcceptAwaiter(KqueueListener& listener) noexcept : listener_(&listener) {}

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

    Result<KqueueStream> result = TryAccept();
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

  Result<KqueueStream> await_resume() noexcept { return result_.Take(); }

private:
  friend class KqueueListener;

  void CompleteInline(Result<KqueueStream> result) noexcept {
    result_.SetResult(std::move(result));
    COROPACT_CHECK(lifecycle_.TryAuthorizeResult(), "Reactor accept result was authorized twice");
    COROPACT_CHECK(lifecycle_.TryAuthorizeRelease(),
                   "Reactor accept release was not authorized after its result");
  }

  [[nodiscard]] bool CompleteResult(Result<KqueueStream> result) noexcept {
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
    Result<KqueueStream> result = TryAccept();
    if (!result.has_value() && IsWouldBlock(result.error().value())) {
      if (!listener_->channel_.IsReading()) {
        listener_->channel_.EnableReading();
      }
      return;
    }
    listener_->CompleteAccept(std::move(result));
  }

private:
  Result<KqueueStream> TryAccept() noexcept {
    int fd = -1;
    net::Endpoint peer_addr(0);
    do {
      fd = listener_->socket_.Accept(&peer_addr);
    } while (fd < 0 && errno == EINTR);

    if (fd < 0) {
      return std::unexpected(CurrentErrno());
    }
    return KqueueStream(listener_->loop_, fd, peer_addr, listener_->stream_options_);
  }

  KqueueListener* listener_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::SingleResultLifecycle lifecycle_;
  backend::detail::ValueResultState<KqueueStream> result_;
};

bool KqueueAcceptSource::NextAwaiter::await_suspend(
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

KqueueAcceptSource::NextResult KqueueAcceptSource::NextAwaiter::await_resume() noexcept {
  return result_.Take();
}

void KqueueAcceptSource::NextAwaiter::Complete(NextResult result) noexcept {
  if (!completion_gate_.TryComplete()) {
    return;
  }
  result_.SetResult(std::move(result));
  continuation_.Schedule();
}

KqueueAcceptSource::KqueueAcceptSource(KqueueListener* listener,
                                         net::detail::AcceptSourceStateMachine state) noexcept
    : listener_(listener), state_(std::move(state)) {}

KqueueAcceptSource::KqueueAcceptSource(KqueueAcceptSource&& other) noexcept
    : listener_(std::exchange(other.listener_, nullptr)),
      state_(std::move(other.state_)),
      events_(std::move(other.events_)),
      terminal_error_(std::move(other.terminal_error_)),
      pending_next_(nullptr) {
  COROPACT_CHECK(other.pending_next_ == nullptr,
                 "KqueueAcceptSource cannot move with a pending Next");
  COROPACT_CHECK(state_.State() != net::detail::AcceptSourceState::kActive &&
                     state_.State() != net::detail::AcceptSourceState::kStopping,
                 "KqueueAcceptSource cannot move while it is running");
  if (listener_ != nullptr && listener_->accept_source_ == &other) {
    listener_->accept_source_ = this;
  }
}

KqueueAcceptSource& KqueueAcceptSource::operator=(KqueueAcceptSource&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  COROPACT_CHECK(pending_next_ == nullptr, "KqueueAcceptSource destination has a pending Next");
  COROPACT_CHECK(state_.State() != net::detail::AcceptSourceState::kActive &&
                     state_.State() != net::detail::AcceptSourceState::kStopping,
                 "KqueueAcceptSource destination is running");
  COROPACT_CHECK(other.pending_next_ == nullptr,
                 "KqueueAcceptSource cannot move with a pending Next");
  COROPACT_CHECK(other.state_.State() != net::detail::AcceptSourceState::kActive &&
                     other.state_.State() != net::detail::AcceptSourceState::kStopping,
                 "KqueueAcceptSource cannot move while it is running");

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

KqueueAcceptSource::~KqueueAcceptSource() {
  if (listener_ == nullptr) {
    return;
  }
  COROPACT_CHECK(listener_->loop_->IsInLoopThread(),
                 "KqueueAcceptSource destructor called from wrong thread");
  COROPACT_CHECK(pending_next_ == nullptr, "KqueueAcceptSource destroyed with a pending Next");
  const auto state = state_.State();
  COROPACT_CHECK(state == net::detail::AcceptSourceState::kIdle ||
                     state == net::detail::AcceptSourceState::kDraining ||
                     state == net::detail::AcceptSourceState::kTerminal,
                 "KqueueAcceptSource destroyed before reaching a safe lifecycle state");
  COROPACT_CHECK(state_.ArmedRequests() == 0, "KqueueAcceptSource destroyed with an armed accept");
  if (listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }
}

coro::Task<Result<void>> KqueueAcceptSource::Stop() {
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

void KqueueAcceptSource::OnReady() noexcept {
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
    Result<KqueueStream> accepted = TryAccept();
    if (!accepted.has_value()) {
      Error error = accepted.error();
      auto completed = state_.CompleteRequest(false);
      COROPACT_CHECK(completed.has_value(),
                     "KqueueAcceptSource: failed to record accept completion");
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
                     "KqueueAcceptSource: failed to record accept completion");
      Fail(Errno(ENOMEM));
      return;
    }

    auto completed = state_.CompleteRequest(true);
    COROPACT_CHECK(completed.has_value(), "KqueueAcceptSource: failed to record accepted stream");
    if (!state_.TryArm()) {
      if (state_.QueuedEvents() >= state_.Options().event_capacity) {
        auto paused = state_.RequestPause();
        COROPACT_CHECK(paused.has_value(), "KqueueAcceptSource: failed to enter the paused state");
      }
      break;
    }
  }

  EnsureAdmission();
  DeliverNextIfReady();
}

void KqueueAcceptSource::OnError(Error error) noexcept {
  if (state_.State() != net::detail::AcceptSourceState::kActive) {
    return;
  }
  if (state_.ArmedRequests() != 0) {
    auto completed = state_.CompleteRequest(false);
    COROPACT_CHECK(completed.has_value(), "KqueueAcceptSource: failed to record error completion");
  }
  Fail(error);
}

void KqueueAcceptSource::OnListenerClosed() noexcept {
  if (listener_->channel_.IsReading()) {
    listener_->channel_.DisableReading();
  }
  if (state_.ArmedRequests() != 0) {
    auto completed = state_.CompleteRequest(false);
    COROPACT_CHECK(completed.has_value(), "KqueueAcceptSource: failed to drain close completion");
  }
  state_.RequestStop();
  DeliverNextIfReady();
}

void KqueueAcceptSource::EnsureAdmission() noexcept {
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

void KqueueAcceptSource::DeliverNextIfReady() noexcept {
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

bool KqueueAcceptSource::TryTakeNext(NextResult& result) noexcept {
  if (!events_.empty()) {
    Event event(std::in_place, std::move(events_.front()));
    events_.pop_front();
    COROPACT_CHECK(state_.ConsumeEvent(),
                   "KqueueAcceptSource: queue and state became inconsistent");
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

void KqueueAcceptSource::ReleaseListenerReservation() noexcept {
  if (listener_ != nullptr && state_.State() == net::detail::AcceptSourceState::kTerminal &&
      listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }
}

void KqueueAcceptSource::Fail(Error error) noexcept {
  if (!terminal_error_.has_value()) {
    terminal_error_ = error;
  }
  if (listener_->channel_.IsReading()) {
    listener_->channel_.DisableReading();
  }
  state_.RequestStop();
  DeliverNextIfReady();
}

Result<KqueueStream> KqueueAcceptSource::TryAccept() noexcept {
  int fd = -1;
  net::Endpoint peer_addr(0);
  do {
    fd = listener_->socket_.Accept(&peer_addr);
  } while (fd < 0 && errno == EINTR);

  if (fd < 0) {
    return std::unexpected(CurrentErrno());
  }
  return KqueueStream(listener_->loop_, fd, peer_addr, listener_->stream_options_);
}

KqueueListener::KqueueListener(KqueueLoop* loop, const net::Endpoint& listen_addr,
                                 KqueueListenerOptions options)
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

KqueueListener::KqueueListener(KqueueLoop* loop, net::Socket socket,
                                 KqueueStreamOptions stream_options) noexcept
    : loop_(CheckLoop(loop)),
      socket_(std::move(socket)),
      channel_(loop_, socket_.fd()),
      stream_options_(stream_options) {
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

Result<KqueueListener> KqueueListener::Create(KqueueLoop* loop,
                                                      const net::Endpoint& listen_addr,
                                                      KqueueListenerOptions options) noexcept {
  if (loop == nullptr) {
    return std::unexpected(Errno(EINVAL));
  }

  COROPACT_TRY_VALUE(socket, TryCreateListenSocket(listen_addr, options));
  return KqueueListener(loop, std::move(socket), options.stream_options);
}

KqueueListener::KqueueListener(KqueueListener&& other) noexcept
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

KqueueListener& KqueueListener::operator=(KqueueListener&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  KqueueLoop* other_loop = PrepareMove(other);
  COROPACT_CHECK(loop_ == nullptr || loop_ == other_loop,
                 "KqueueListener move requires both objects to use the same KqueueLoop");
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

KqueueListener::~KqueueListener() {
  if (loop_ == nullptr) {
    return;
  }
  RequireOwnerLoop();
  COROPACT_CHECK(pending_accept_ == nullptr, "KqueueListener destroyed with a pending accept");
  COROPACT_CHECK(accept_source_ == nullptr,
                 "KqueueListener destroyed with an active AcceptSource");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
}

coro::Task<Result<KqueueStream>> KqueueListener::Accept() {
  RequireOwnerLoop();
  if (closed_) {
    co_return std::unexpected(Errno(EBADF));
  }
  if (accept_source_ != nullptr) {
    co_return std::unexpected(Errno(EBUSY));
  }
  co_return co_await AcceptAwaiter(*this);
}

Result<KqueueAcceptSource> KqueueListener::AcceptSource(
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
  return KqueueAcceptSource(this, std::move(state));
}

coro::Task<Result<void>> KqueueListener::Close() {
  RequireOwnerLoop();
  CloseNow();
  co_return Result<void>{};
}

void KqueueListener::CloseNow() noexcept {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "KqueueListener::CloseNow called from wrong thread");
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

Result<net::Endpoint> KqueueListener::LocalAddress() const {
  RequireOwnerLoop();
  if (closed_) {
    return std::unexpected(Errno(EBADF));
  }
  return socket_.LocalEndpoint();
}

void KqueueListener::HandleRead() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "KqueueListener::HandleRead called from wrong thread");
  if (pending_accept_ != nullptr) {
    pending_accept_->OnReady();
  } else if (accept_source_ != nullptr) {
    accept_source_->OnReady();
  }
}

void KqueueListener::HandleError() {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "KqueueListener::HandleError called from wrong thread");
  if (pending_accept_ != nullptr) {
    CompleteAccept(std::unexpected(SocketError(socket_.fd())));
  } else if (accept_source_ != nullptr) {
    accept_source_->OnError(SocketError(socket_.fd()));
  }
}

void KqueueListener::DispatchRead(void* context) noexcept {
  static_cast<KqueueListener*>(context)->HandleRead();
}

void KqueueListener::DispatchError(void* context) noexcept {
  static_cast<KqueueListener*>(context)->HandleError();
}

void KqueueListener::CompleteAccept(Result<KqueueStream> result) {
  COROPACT_DCHECK(loop_->IsInLoopThread(),
                  "KqueueListener::CompleteAccept called from wrong thread");
  AcceptAwaiter* awaiter = pending_accept_;
  if (awaiter == nullptr) {
    return;
  }
  COROPACT_CHECK(awaiter->CompleteResult(std::move(result)),
                 "KqueueListener::CompleteAccept result was already authorized");
  COROPACT_CHECK(awaiter->TryAuthorizeRelease(),
                 "KqueueListener::CompleteAccept release was not authorized after its result");

  AcceptAwaiter* released = std::exchange(pending_accept_, nullptr);
  COROPACT_CHECK(released == awaiter,
                 "KqueueListener::CompleteAccept pending slot changed during completion");
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  COROPACT_CHECK(awaiter->TryAuthorizeContinuation(),
                 "KqueueListener::CompleteAccept continuation was not authorized after release");
  awaiter->ScheduleContinuation();
}

void KqueueListener::DetachChannel() {
  COROPACT_DCHECK(loop_->IsInLoopThread(),
                  "KqueueListener::DetachChannel called from wrong thread");
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (channel_.IsRegistered()) {
    channel_.Remove();
  }
}

void KqueueListener::RequireOwnerLoop() const noexcept {
  COROPACT_CHECK(loop_ != nullptr, "KqueueListener operation has no owner KqueueLoop");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "KqueueListener operation called from wrong KqueueLoop thread");
}

void KqueueListener::BindChannelCallbacks() noexcept {
  try {
    channel_.SetTriggerMode(TriggerMode::kOneShot);
    channel_.SetReadCallback(&KqueueListener::DispatchRead, this);
    channel_.SetErrorCallback(&KqueueListener::DispatchError, this);
  } catch (...) {
    COROPACT_CHECK(false, "KqueueListener: failed to bind channel callbacks");
  }
}

void KqueueListener::ResetForMove() noexcept {
  COROPACT_CHECK(loop_ != nullptr, "KqueueListener move destination is not initialized");
  COROPACT_CHECK(loop_->IsInLoopThread(),
                 "KqueueListener move called from wrong KqueueLoop thread");
  COROPACT_CHECK(pending_accept_ == nullptr,
                 "KqueueListener move destination has a pending accept");
  COROPACT_CHECK(accept_source_ == nullptr,
                 "KqueueListener move destination has an active AcceptSource");
  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
  socket_.Close();
}

KqueueLoop* KqueueListener::PrepareMove(KqueueListener& other) noexcept {
  COROPACT_CHECK(other.loop_ != nullptr, "KqueueListener move source is not initialized");
  COROPACT_CHECK(other.loop_->IsInLoopThread(),
                 "KqueueListener move called from wrong KqueueLoop thread");
  COROPACT_CHECK(other.pending_accept_ == nullptr,
                 "KqueueListener cannot move with a pending accept operation");
  COROPACT_CHECK(other.accept_source_ == nullptr,
                 "KqueueListener cannot move with an active AcceptSource");

  other.DetachChannel();
  LoopAccess::UnregisterShutdownParticipant(*other.loop_, other.shutdown_participant_);
  KqueueLoop* loop = other.loop_;
  other.loop_ = nullptr;
  return loop;
}

void KqueueListener::DispatchLoopStop(void* context) noexcept {
  static_cast<KqueueListener*>(context)->CloseNow();
}

}  // namespace coropact::kqueue
