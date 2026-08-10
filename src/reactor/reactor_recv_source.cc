// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/reactor/recv_source.h"

#include <sys/socket.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <coroutine>
#include <expected>
#include <limits>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/reactor/detail/loop_access.h"

namespace coropact::reactor {

using detail::LoopAccess;

namespace {

bool IsWouldBlock(int error) noexcept {
  return error == EAGAIN || error == EWOULDBLOCK;
}

base::Error SocketError(int fd) noexcept {
  int error = 0;
  auto length = static_cast<socklen_t>(sizeof(error));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) < 0) {
    return base::CurrentErrno();
  }
  return base::MakeErrno(error == 0 ? EIO : error);
}

}  // namespace

bool ReactorRecvSourceOptions::Valid() const noexcept {
  return source.Valid() && source.pending_depth == 1 && buffer_size > 0 &&
         source.buffer_capacity <= std::numeric_limits<std::uint32_t>::max();
}

class ReactorRecvSource::NextAwaiter {
public:
  explicit NextAwaiter(ReactorRecvSource& source) noexcept : source_(&source) {}

  [[nodiscard]]
  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (source_->pending_next_ != nullptr) {
      result_.emplace(std::unexpected(base::MakeErrno(EBUSY)));
      (void)(completion_gate_.TryComplete());
      return false;
    }

    continuation_.Bind(continuation);

    ReactorRecvSource::Result result;
    if (source_->TryTakeNext(result)) {
      result_.emplace(std::move(result));
      (void)(completion_gate_.TryComplete());
      return false;
    }

    source_->pending_next_ = this;
    source_->EnsureAdmission();

    // A level-triggered readiness source can already have data available when
    // admission is enabled. Recheck before parking the coroutine.
    if (source_->TryTakeNext(result)) {
      source_->pending_next_ = nullptr;
      result_.emplace(std::move(result));
      (void)(completion_gate_.TryComplete());
      return false;
    }
    return true;
  }

  ReactorRecvSource::Result await_resume() noexcept {
    assert(result_.has_value());
    return std::move(*result_);
  }

  void Complete(ReactorRecvSource::Result result) noexcept {
    if (!completion_gate_.TryComplete()) {
      return;
    }
    result_.emplace(std::move(result));
    continuation_.Schedule();
  }

private:
  ReactorRecvSource* source_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  std::optional<ReactorRecvSource::Result> result_;
};

class ReactorRecvSource::StopAwaiter {
public:
  explicit StopAwaiter(ReactorRecvSource& source) noexcept : source_(&source) {}

  [[nodiscard]]
  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (source_->pending_stop_ != nullptr) {
      result_.emplace(std::unexpected(base::MakeErrno(EBUSY)));
      (void)(completion_gate_.TryComplete());
      return false;
    }

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
      result_.emplace(base::Result<void>{});
      (void)(completion_gate_.TryComplete());
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
  ReactorRecvSource* source_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  std::optional<base::Result<void>> result_;
};

base::Result<ReactorRecvSource> ReactorRecvSource::Create(
    EventLoop* loop,
    int fd,
    ReactorRecvSourceOptions options) noexcept {
  if (loop == nullptr || fd < 0 || !loop->IsInLoopThread()) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (!options.Valid()) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (options.source.buffer_capacity >
      std::numeric_limits<std::size_t>::max() / options.buffer_size) {
    return std::unexpected(base::MakeErrno(EOVERFLOW));
  }

  auto state = net::detail::RecvSourceStateMachine::Create(options.source);
  if (!state.has_value()) {
    return std::unexpected(state.error());
  }

  std::vector<std::byte> storage;
  std::vector<std::uint32_t> available_buffers;
  try {
    storage.resize(options.source.buffer_capacity * options.buffer_size);
    available_buffers.reserve(options.source.buffer_capacity);
    for (std::size_t i = 0; i < options.source.buffer_capacity; ++i) {
      available_buffers.push_back(static_cast<std::uint32_t>(i));
    }
  } catch (...) {
    return std::unexpected(base::MakeErrno(ENOMEM));
  }

  return ReactorRecvSource(
      loop,
      fd,
      std::move(*state),
      options.buffer_size,
      std::move(storage),
      std::move(available_buffers));
}

ReactorRecvSource::ReactorRecvSource(
    EventLoop* loop,
    int fd,
    net::detail::RecvSourceStateMachine state,
    std::size_t buffer_size,
    std::vector<std::byte> storage,
    std::vector<std::uint32_t> available_buffers) noexcept
    : loop_(loop),
      fd_(fd),
      state_(std::move(state)),
      channel_(loop, fd),
      buffer_size_(buffer_size),
      storage_(std::move(storage)),
      available_buffers_(std::move(available_buffers)) {
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

ReactorRecvSource::~ReactorRecvSource() {
  if (loop_ == nullptr) {
    return;
  }

  COROPACT_DCHECK(loop_->IsInLoopThread(),
                 "ReactorRecvSource destroyed from wrong thread");
  COROPACT_DCHECK(pending_next_ == nullptr,
                 "ReactorRecvSource destroyed with pending Next");
  COROPACT_DCHECK(pending_stop_ == nullptr,
                 "ReactorRecvSource destroyed with pending Stop");
  COROPACT_DCHECK(state_.State() == net::detail::RecvSourceState::kIdle ||
                     state_.State() == net::detail::RecvSourceState::kTerminal,
                 "ReactorRecvSource destroyed before Stop completed");
  COROPACT_DCHECK(events_.empty(),
                 "ReactorRecvSource destroyed with queued events");
  COROPACT_DCHECK(state_.OutstandingLeases() == 0,
                 "ReactorRecvSource destroyed with outstanding leases");
  COROPACT_DCHECK(available_buffers_.size() == state_.Options().buffer_capacity,
                 "ReactorRecvSource destroyed with a missing buffer");

  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
}

ReactorRecvSource::ReactorRecvSource(ReactorRecvSource&& other) noexcept
    : loop_(other.loop_),
      fd_(std::exchange(other.fd_, -1)),
      state_(std::move(other.state_)),
      channel_(std::move(other.channel_)),
      events_(std::move(other.events_)),
      terminal_error_(std::move(other.terminal_error_)),
      buffer_size_(std::exchange(other.buffer_size_, 0)),
      storage_(std::move(other.storage_)),
      available_buffers_(std::move(other.available_buffers_)) {
  COROPACT_CHECK(other.pending_next_ == nullptr,
                 "ReactorRecvSource cannot move with pending Next");
  COROPACT_CHECK(other.pending_stop_ == nullptr,
                 "ReactorRecvSource cannot move with pending Stop");
  COROPACT_CHECK(other.state_.State() == net::detail::RecvSourceState::kIdle ||
                     other.state_.State() == net::detail::RecvSourceState::kTerminal,
                 "ReactorRecvSource cannot move while active");
  COROPACT_CHECK(other.events_.empty(),
                 "ReactorRecvSource cannot move with queued events");
  COROPACT_CHECK(other.state_.OutstandingLeases() == 0,
                 "ReactorRecvSource cannot move with outstanding leases");
  LoopAccess::UnregisterShutdownParticipant(*other.loop_, other.shutdown_participant_);
  other.loop_ = nullptr;
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

ReactorRecvSource& ReactorRecvSource::operator=(ReactorRecvSource&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  COROPACT_CHECK(pending_next_ == nullptr,
                 "ReactorRecvSource destination has pending Next");
  COROPACT_CHECK(pending_stop_ == nullptr,
                 "ReactorRecvSource destination has pending Stop");
  COROPACT_CHECK(state_.State() == net::detail::RecvSourceState::kIdle ||
                     state_.State() == net::detail::RecvSourceState::kTerminal,
                 "ReactorRecvSource destination is active");
  COROPACT_CHECK(events_.empty(),
                 "ReactorRecvSource destination has queued events");
  COROPACT_CHECK(state_.OutstandingLeases() == 0,
                 "ReactorRecvSource destination has outstanding leases");

  COROPACT_CHECK(other.pending_next_ == nullptr,
                 "ReactorRecvSource source has pending Next");
  COROPACT_CHECK(other.pending_stop_ == nullptr,
                 "ReactorRecvSource source has pending Stop");
  COROPACT_CHECK(other.state_.State() == net::detail::RecvSourceState::kIdle ||
                     other.state_.State() == net::detail::RecvSourceState::kTerminal,
                 "ReactorRecvSource source is active");
  COROPACT_CHECK(other.events_.empty(),
                 "ReactorRecvSource source has queued events");
  COROPACT_CHECK(other.state_.OutstandingLeases() == 0,
                 "ReactorRecvSource source has outstanding leases");

  if (loop_ != nullptr) {
    LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  }
  DetachChannel();
  LoopAccess::UnregisterShutdownParticipant(*other.loop_, other.shutdown_participant_);
  loop_ = std::exchange(other.loop_, nullptr);
  fd_ = std::exchange(other.fd_, -1);
  state_ = std::move(other.state_);
  channel_ = std::move(other.channel_);
  terminal_error_ = std::move(other.terminal_error_);
  buffer_size_ = std::exchange(other.buffer_size_, 0);
  storage_ = std::move(other.storage_);
  available_buffers_ = std::move(other.available_buffers_);
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
  return *this;
}

base::Result<void> ReactorRecvSource::Start() noexcept {
  if (state_.State() != net::detail::RecvSourceState::kIdle) {
    return std::unexpected(base::MakeErrno(EALREADY));
  }

  auto started = state_.Start();
  if (!started.has_value()) {
    return started;
  }
  EnsureAdmission();
  if (state_.ArmedRequests() == 0) {
    return std::unexpected(base::MakeErrno(ENOBUFS));
  }
  return {};
}

base::Result<bool> ReactorRecvSource::BeginStop() noexcept {
  auto stopped = state_.RequestStop();
  if (!stopped.has_value()) {
    return std::unexpected(stopped.error());
  }

  CompleteReadiness();
  DeliverNextIfReady();

  return state_.State() != net::detail::RecvSourceState::kTerminal;
}

void ReactorRecvSource::EnsureAdmission() noexcept {
  if (loop_ == nullptr || !loop_->IsInLoopThread() ||
      state_.State() != net::detail::RecvSourceState::kActive ||
      state_.ArmedRequests() != 0) {
    return;
  }
  if (state_.TryArm()) {
    channel_.EnableReading();
  }
}

void ReactorRecvSource::CompleteReadiness() noexcept {
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  if (state_.ArmedRequests() != 0) {
    auto completed = state_.CompleteMultishotEvent(
        net::detail::EventDisposition::kNone,
        net::detail::MultishotRequestDisposition::kTerminal);
    COROPACT_CHECK(completed.has_value(),
                   "ReactorRecvSource failed to complete readiness request");
  }
}

void ReactorRecvSource::RequestBackendStop(std::optional<base::Error> error) noexcept {
  if (error.has_value() && !terminal_error_.has_value()) {
    terminal_error_ = *error;
  }

  auto stopped = state_.RequestStop();
  COROPACT_CHECK(stopped.has_value(),
                 "ReactorRecvSource failed to enter stopping state");
  CompleteReadiness();
  DeliverNextIfReady();
  CompleteStopIfReady();
}

void ReactorRecvSource::OnReady() noexcept {
  if (state_.State() != net::detail::RecvSourceState::kActive ||
      state_.ArmedRequests() == 0) {
    return;
  }

  while (state_.State() == net::detail::RecvSourceState::kActive) {
    if (!state_.CanQueueEvent() || available_buffers_.empty()) {
      RequestBackendStop(base::MakeErrno(ENOBUFS));
      return;
    }

    const std::uint32_t buffer_id = available_buffers_.back();
    available_buffers_.pop_back();
    auto* data = storage_.data() + static_cast<std::size_t>(buffer_id) * buffer_size_;

    ssize_t received = -1;
    do {
      received = ::recv(fd_, data, buffer_size_, MSG_DONTWAIT);
    } while (received < 0 && errno == EINTR);

    if (received < 0) {
      const int error = errno;
      available_buffers_.push_back(buffer_id);
      if (IsWouldBlock(error)) {
        return;
      }
      RequestBackendStop(base::MakeErrno(error));
      return;
    }

    if (received == 0) {
      available_buffers_.push_back(buffer_id);
      RequestBackendStop();
      return;
    }

    auto recorded = state_.CompleteMultishotEvent(
        net::detail::EventDisposition::kProduced,
        net::detail::MultishotRequestDisposition::kMore);
    if (!recorded.has_value()) {
      available_buffers_.push_back(buffer_id);
      RequestBackendStop(recorded.error());
      return;
    }

    net::BufferLease lease(
        data,
        static_cast<std::size_t>(received),
        buffer_id,
        this,
        &ReclaimBuffer);
    try {
      events_.push_back(Event{.buffer = std::move(lease)});
    } catch (...) {
      // The temporary lease returns the buffer and decrements the outstanding
      // lease count before the queue reservation is rolled back.
      COROPACT_CHECK(state_.DiscardQueuedEvent(),
                     "ReactorRecvSource failed to roll back queue reservation");
      RequestBackendStop(base::MakeErrno(ENOMEM));
      return;
    }

    if (pending_next_ != nullptr) {
      DeliverNextIfReady();
      return;
    }
  }
}

void ReactorRecvSource::OnClose() noexcept {
  RequestBackendStop();
}

void ReactorRecvSource::OnError() noexcept {
  RequestBackendStop(SocketError(fd_));
}

void ReactorRecvSource::DeliverNextIfReady() noexcept {
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

void ReactorRecvSource::CompleteStopIfReady() noexcept {
  if (pending_stop_ == nullptr ||
      state_.State() != net::detail::RecvSourceState::kTerminal ||
      state_.ArmedRequests() != 0) {
    return;
  }

  auto* awaiter = std::exchange(pending_stop_, nullptr);
  awaiter->Complete(base::Result<void>{});
}

bool ReactorRecvSource::TryTakeNext(Result& result) noexcept {
  if (!events_.empty()) {
    Event event = std::move(events_.front());
    events_.pop_front();
    COROPACT_CHECK(state_.AcquireEvent(),
                   "ReactorRecvSource queue and state became inconsistent");
    result = Result(std::in_place, std::move(event));
    EnsureAdmission();
    return true;
  }

  if (state_.State() == net::detail::RecvSourceState::kTerminal) {
    if (terminal_error_.has_value()) {
      result = std::unexpected(*terminal_error_);
    } else {
      result = Result(std::in_place, std::nullopt);
    }
    return true;
  }
  return false;
}

void ReactorRecvSource::ReturnBuffer(std::uint32_t buffer_id) noexcept {
  COROPACT_CHECK(loop_ != nullptr && loop_->IsInLoopThread(),
                 "ReactorRecvSource buffer returned from wrong thread");
  COROPACT_CHECK(buffer_id < state_.Options().buffer_capacity,
                 "ReactorRecvSource invalid buffer id");
  COROPACT_CHECK(available_buffers_.size() < state_.Options().buffer_capacity,
                 "ReactorRecvSource buffer returned twice");
  available_buffers_.push_back(buffer_id);
  COROPACT_CHECK(state_.ReleaseLease(),
                 "ReactorRecvSource lease released twice");
  EnsureAdmission();
  DeliverNextIfReady();
  CompleteStopIfReady();
}

void ReactorRecvSource::ReclaimBuffer(
    void* context,
    std::uint32_t buffer_id) noexcept {
  static_cast<ReactorRecvSource*>(context)->ReturnBuffer(buffer_id);
}

void ReactorRecvSource::DetachChannel() noexcept {
  if (loop_ == nullptr || !loop_->IsInLoopThread()) {
    return;
  }
  if (!channel_.IsNoneEvent()) {
    channel_.DisableAll();
  }
  if (channel_.IsRegistered()) {
    channel_.Remove();
  }
}

void ReactorRecvSource::BindChannelCallbacks() noexcept {
  channel_.SetReadCallback(
      [](void* context) noexcept {
        static_cast<ReactorRecvSource*>(context)->OnReady();
      },
      this);
  channel_.SetCloseCallback(
      [](void* context) noexcept {
        static_cast<ReactorRecvSource*>(context)->OnClose();
      },
      this);
  channel_.SetErrorCallback(
      [](void* context) noexcept {
        static_cast<ReactorRecvSource*>(context)->OnError();
      },
      this);
}

void ReactorRecvSource::DispatchLoopStop(void* context) noexcept {
  static_cast<ReactorRecvSource*>(context)->RequestBackendStop();
}

coro::Task<ReactorRecvSource::Result> ReactorRecvSource::Next() {
  if (loop_ == nullptr || fd_ < 0) {
    co_return std::unexpected(base::MakeErrno(EBADF));
  }
  if (!loop_->IsInLoopThread()) {
    co_return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (pending_next_ != nullptr) {
    co_return std::unexpected(base::MakeErrno(EBUSY));
  }

  if (state_.State() == net::detail::RecvSourceState::kIdle) {
    if (loop_->State() == backend::LoopState::kStopping ||
        loop_->State() == backend::LoopState::kStopped) {
      co_return std::unexpected(base::MakeErrno(ECANCELED));
    }
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

base::Result<void> ReactorRecvSource::RequestStop() noexcept {
  if (loop_ == nullptr) {
    return {};
  }
  if (!loop_->IsInLoopThread()) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  auto waiting = BeginStop();
  if (!waiting.has_value()) {
    return std::unexpected(waiting.error());
  }
  return {};
}

coro::Task<base::Result<void>> ReactorRecvSource::Stop() {
  if (loop_ == nullptr) {
    co_return base::Result<void>{};
  }
  if (!loop_->IsInLoopThread()) {
    co_return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (pending_stop_ != nullptr) {
    co_return std::unexpected(base::MakeErrno(EBUSY));
  }
  co_return co_await StopAwaiter(*this);
}

}  // namespace coropact::reactor
