// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/reactor/reactor_listener.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <coroutine>
#include <expected>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/base/try.h"
#include "coropact/net/accept_source.h"
#include "coropact/net/net_utils.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/reactor/detail/result_state.h"

namespace coropact::reactor {
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
  return base::MakeErrno(err);
}

EventLoop* CheckLoop(EventLoop* loop) noexcept {
  COROPACT_CHECK(loop != nullptr, "ReactorListener: loop must not be null");
  return loop;
}

base::Result<net::Socket> TryCreateListenSocket(const net::Endpoint& listen_addr,
                                                ReactorListenerOptions options) noexcept {
  net::Socket socket(COROPACT_TRY(net::CreateNonBlockingSocket(listen_addr.native_family())));

  COROPACT_TRY(net::set_reuse_addr(socket.fd(), options.reuse_addr));

  if (options.reuse_port) {
    COROPACT_TRY(net::set_reuse_port(socket.fd(), true));
  }

  if (::bind(socket.fd(), listen_addr.sock_addr(), listen_addr.sock_addr_len()) < 0) {
    return std::unexpected(base::CurrentErrno());
  }

  if (::listen(socket.fd(), SOMAXCONN) < 0) {
    return std::unexpected(base::CurrentErrno());
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
    COROPACT_DCHECK(listener_->loop_->IsInLoopThread(), "AcceptAwaiter: wrong EventLoop thread");
    COROPACT_DCHECK(listener_->pending_accept_ == nullptr,
                    "AcceptAwaiter: only one pending accept is supported per listener");

    continuation_.Bind(continuation);

    base::Result<ReactorStream> result = TryAccept();
    if (result.has_value() || !IsWouldBlock(result.error().value())) {
      result_.SetResult(std::move(result));
      static_cast<void>(completion_gate_.TryComplete());
      return false;
    }

    listener_->pending_accept_ = this;
    if (!listener_->channel_.IsReading()) {
      listener_->channel_.EnableReading();
    }
    return true;
  }

  base::Result<ReactorStream> await_resume() noexcept {
    COROPACT_DCHECK(result_.HasResult(), "AcceptAwaiter: result is not ready");
    return result_.Take();
  }

  void Complete(base::Result<ReactorStream> result) noexcept {
    if (!completion_gate_.TryComplete()) {
      return;
    }
    result_.SetResult(std::move(result));
    continuation_.Schedule();
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
    net::Endpoint peer_addr(0);
    do {
      fd = listener_->socket_.Accept(&peer_addr);
    } while (fd < 0 && errno == EINTR);

    if (fd < 0) {
      return std::unexpected(base::CurrentErrno());
    }
    return ReactorStream(listener_->loop_, fd, peer_addr);
  }

  ReactorListener* listener_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  detail::ReactorValueResultState<ReactorStream> result_;
};

class ReactorAcceptSource::NextAwaiter {
public:
  using Result = ReactorAcceptSource::Result;

  explicit NextAwaiter(ReactorAcceptSource& source) noexcept : source_(&source) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    COROPACT_DCHECK(source_->listener_ != nullptr,
                    "ReactorAcceptSource::Next: source has no listener");
    COROPACT_DCHECK(source_->listener_->loop_->IsInLoopThread(),
                    "ReactorAcceptSource::Next: wrong EventLoop thread");
    COROPACT_DCHECK(source_->pending_next_ == nullptr,
                    "ReactorAcceptSource::Next: only one pending Next is supported");

    continuation_.Bind(continuation);

    Result result;
    if (source_->TryTakeNext(result)) {
      result_.SetResult(std::move(result));
      static_cast<void>(completion_gate_.TryComplete());
      return false;
    }

    source_->pending_next_ = this;
    source_->EnsureAdmission();

    // Admission may complete synchronously in a readiness backend. Recheck
    // after arming so a just-available event does not leave the coroutine
    // parked until a second poll notification.
    if (source_->TryTakeNext(result)) {
      source_->pending_next_ = nullptr;
      result_.SetResult(std::move(result));
      static_cast<void>(completion_gate_.TryComplete());
      return false;
    }
    return true;
  }

  Result await_resume() noexcept {
    COROPACT_DCHECK(result_.HasResult(), "ReactorAcceptSource::Next: result is not ready");
    return result_.Take();
  }

  void Complete(Result result) noexcept {
    if (!completion_gate_.TryComplete()) {
      return;
    }
    result_.SetResult(std::move(result));
    continuation_.Schedule();
  }

private:
  ReactorAcceptSource* source_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  detail::ReactorValueResultState<std::optional<ReactorStream>> result_;
};

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
  COROPACT_DCHECK(listener_->loop_->IsInLoopThread(),
                  "ReactorAcceptSource destructor called from wrong thread");
  COROPACT_DCHECK(pending_next_ == nullptr, "ReactorAcceptSource destroyed with a pending Next");
  COROPACT_DCHECK(state_.State() != net::detail::AcceptSourceState::kActive &&
                      state_.State() != net::detail::AcceptSourceState::kStopping,
                  "ReactorAcceptSource destroyed while it is running");
  COROPACT_DCHECK(state_.ArmedRequests() == 0,
                  "ReactorAcceptSource destroyed with an armed accept");
  if (listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }
}

coro::Task<ReactorAcceptSource::Result> ReactorAcceptSource::Next() {
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
    if (listener_->closed_) {
      auto stopped = state_.RequestStop();
      COROPACT_CHECK(stopped.has_value(),
                     "ReactorAcceptSource::Next: failed to stop closed source");
      co_return Event{};
    }
    if (listener_->pending_accept_ != nullptr ||
        (listener_->accept_source_ != nullptr && listener_->accept_source_ != this)) {
      co_return std::unexpected(base::MakeErrno(EBUSY));
    }
    auto started = state_.Start();
    if (!started.has_value()) {
      co_return std::unexpected(started.error());
    }
    listener_->accept_source_ = this;
  }

  Result result;
  if (TryTakeNext(result)) {
    co_return result;
  }
  co_return co_await NextAwaiter(*this);
}

coro::Task<base::Result<void>> ReactorAcceptSource::Stop() {
  if (listener_ == nullptr) {
    co_return base::Result<void>{};
  }
  COROPACT_DCHECK(listener_->loop_->IsInLoopThread(),
                  "ReactorAcceptSource::Stop called from wrong thread");

  if (state_.State() == net::detail::AcceptSourceState::kIdle) {
    auto stopped = state_.RequestStop();
    if (!stopped.has_value()) {
      co_return std::unexpected(stopped.error());
    }
    ReleaseListenerReservation();
    co_return base::Result<void>{};
  }

  if (state_.State() == net::detail::AcceptSourceState::kActive ||
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
    auto stopped = state_.RequestStop();
    if (!stopped.has_value()) {
      co_return std::unexpected(stopped.error());
    }
  }

  DeliverNextIfReady();
  ReleaseListenerReservation();
  co_return base::Result<void>{};
}

void ReactorAcceptSource::OnReady(coropact::time::Timestamp /*receive_time*/) noexcept {
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
    base::Result<ReactorStream> accepted = TryAccept();
    if (!accepted.has_value()) {
      base::Error error = accepted.error();
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
      Fail(base::MakeErrno(ENOMEM));
      return;
    }

    auto completed = state_.CompleteRequest(true);
    COROPACT_CHECK(completed.has_value(), "ReactorAcceptSource: failed to record accepted stream");
    if (!state_.TryArm()) {
      break;
    }
  }

  EnsureAdmission();
  DeliverNextIfReady();
}

void ReactorAcceptSource::OnError(base::Error error) noexcept {
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
  auto stopped = state_.RequestStop();
  COROPACT_CHECK(stopped.has_value(), "ReactorAcceptSource: failed to stop on close");
  DeliverNextIfReady();
}

void ReactorAcceptSource::EnsureAdmission() noexcept {
  if (listener_ == nullptr || listener_->closed_ ||
      state_.State() != net::detail::AcceptSourceState::kActive) {
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

  Result result;
  if (!TryTakeNext(result)) {
    return;
  }

  NextAwaiter* awaiter = std::exchange(pending_next_, nullptr);
  awaiter->Complete(std::move(result));
}

bool ReactorAcceptSource::TryTakeNext(Result& result) noexcept {
  if (!events_.empty()) {
    Event event(std::in_place, std::move(events_.front()));
    events_.pop_front();
    COROPACT_CHECK(state_.ConsumeEvent(),
                   "ReactorAcceptSource: queue and state became inconsistent");
    result = Result(std::in_place, std::move(event));
    EnsureAdmission();
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

void ReactorAcceptSource::ReleaseListenerReservation() noexcept {
  if (listener_ != nullptr && state_.State() == net::detail::AcceptSourceState::kTerminal &&
      listener_->accept_source_ == this) {
    listener_->accept_source_ = nullptr;
  }
}

void ReactorAcceptSource::Fail(base::Error error) noexcept {
  if (!terminal_error_.has_value()) {
    terminal_error_ = error;
  }
  if (listener_->channel_.IsReading()) {
    listener_->channel_.DisableReading();
  }
  auto stopped = state_.RequestStop();
  COROPACT_CHECK(stopped.has_value(), "ReactorAcceptSource: failed to enter terminal state");
  DeliverNextIfReady();
}

base::Result<ReactorStream> ReactorAcceptSource::TryAccept() noexcept {
  int fd = -1;
  net::Endpoint peer_addr(0);
  do {
    fd = listener_->socket_.Accept(&peer_addr);
  } while (fd < 0 && errno == EINTR);

  if (fd < 0) {
    return std::unexpected(base::CurrentErrno());
  }
  return ReactorStream(listener_->loop_, fd, peer_addr);
}

ReactorListener::ReactorListener(EventLoop* loop, const net::Endpoint& listen_addr,
                                 ReactorListenerOptions options)
    : loop_(CheckLoop(loop)),
      socket_(CreateListenSocket(listen_addr.native_family())),
      channel_(loop_, socket_.fd()) {
  socket_.set_reuse_addr(options.reuse_addr);
  if (options.reuse_port) {
    socket_.set_reuse_port(true);
  }
  socket_.BindAddress(listen_addr);
  socket_.Listen();

  BindChannelCallbacks();
}

ReactorListener::ReactorListener(EventLoop* loop, net::Socket socket) noexcept
    : loop_(CheckLoop(loop)), socket_(std::move(socket)), channel_(loop_, socket_.fd()) {
  BindChannelCallbacks();
}

base::Result<ReactorListener> ReactorListener::Create(EventLoop* loop,
                                                      const net::Endpoint& listen_addr,
                                                      ReactorListenerOptions options) noexcept {
  if (loop == nullptr) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }

  return ReactorListener(loop, COROPACT_TRY(TryCreateListenSocket(listen_addr, options)));
}

ReactorListener::ReactorListener(ReactorListener&& other) noexcept
    : loop_(PrepareMove(other)),
      socket_(std::move(other.socket_)),
      channel_(std::move(other.channel_)),
      pending_accept_(nullptr),
      accept_source_(nullptr),
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
  accept_source_ = nullptr;
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
  COROPACT_DCHECK(accept_source_ == nullptr,
                  "ReactorListener destroyed with an active AcceptSource");
  DetachChannel();
}

coro::Task<base::Result<ReactorStream>> ReactorListener::Accept() {
  if (closed_) {
    co_return std::unexpected(base::MakeErrno(EBADF));
  }
  if (accept_source_ != nullptr) {
    co_return std::unexpected(base::MakeErrno(EBUSY));
  }
  co_return co_await AcceptAwaiter(*this);
}

base::Result<ReactorAcceptSource> ReactorListener::AcceptSource(
    net::AcceptSourceOptions options) noexcept {
  if (closed_) {
    return std::unexpected(base::MakeErrno(EBADF));
  }
  if (pending_accept_ != nullptr || accept_source_ != nullptr) {
    return std::unexpected(base::MakeErrno(EBUSY));
  }
  auto state = COROPACT_TRY(net::detail::AcceptSourceStateMachine::Create(options));
  return ReactorAcceptSource(this, std::move(state));
}

coro::Task<base::Result<void>> ReactorListener::Close() {
  if (closed_) {
    co_return base::Result<void>{};
  }

  closed_ = true;
  if (pending_accept_ != nullptr) {
    CompleteAccept(std::unexpected(base::MakeErrno(ECANCELED)));
  }
  if (accept_source_ != nullptr) {
    accept_source_->OnListenerClosed();
  }
  DetachChannel();
  socket_.Close();
  co_return base::Result<void>{};
}

base::Result<net::Endpoint> ReactorListener::LocalAddress() const {
  if (closed_) {
    return std::unexpected(base::MakeErrno(EBADF));
  }
  return net::get_local_addr(socket_.fd());
}

void ReactorListener::HandleRead(time::Timestamp receive_time) {
  COROPACT_DCHECK(loop_->IsInLoopThread(), "ReactorListener::HandleRead called from wrong thread");
  if (pending_accept_ != nullptr) {
    pending_accept_->OnReady();
  } else if (accept_source_ != nullptr) {
    accept_source_->OnReady(receive_time);
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

void ReactorListener::DispatchRead(void* context, time::Timestamp receive_time) noexcept {
  static_cast<ReactorListener*>(context)->HandleRead(receive_time);
}

void ReactorListener::DispatchError(void* context) noexcept {
  static_cast<ReactorListener*>(context)->HandleError();
}

void ReactorListener::CompleteAccept(base::Result<ReactorStream> result) {
  COROPACT_DCHECK(loop_->IsInLoopThread(),
                  "ReactorListener::CompleteAccept called from wrong thread");
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
  COROPACT_DCHECK(loop_->IsInLoopThread(),
                  "ReactorListener::DetachChannel called from wrong thread");
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (loop_->HasChannel(&channel_)) {
    channel_.Remove();
  }
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
  EventLoop* loop = other.loop_;
  other.loop_ = nullptr;
  return loop;
}

}  // namespace coropact::reactor
