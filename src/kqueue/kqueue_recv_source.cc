// SPDX-License-Identifier: MIT
#include <sys/socket.h>

#include <algorithm>
#include <cerrno>
#include <coroutine>
#include <expected>
#include <limits>
#include <utility>

#include "coropact/backend/detail/value_result_state.h"
#include "coropact/base/check.h"
#include "coropact/result.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/kqueue/detail/loop_access.h"
#include "coropact/kqueue/options.h"
#include "coropact/kqueue/recv_source.h"

namespace coropact::kqueue {

using detail::LoopAccess;

namespace {

bool IsWouldBlock(int error) noexcept { return error == EAGAIN || error == EWOULDBLOCK; }

Error SocketError(int fd) noexcept {
  int error = 0;
  auto length = static_cast<socklen_t>(sizeof(error));
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) < 0) {
    return CurrentErrno();
  }
  return Errno(error == 0 ? EIO : error);
}

}  // namespace

bool KqueueRecvSourceOptions::Valid() const noexcept {
  return source.Valid() && source.pending_depth == 1 && buffer_size > 0 &&
         source.buffer_capacity <= std::numeric_limits<std::uint32_t>::max();
}

bool KqueueRecvSource::NextAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
  if (source_->loop_ == nullptr || source_->fd_ < 0) {
    result_.SetError(Errno(EBADF));
    (void)(completion_gate_.TryComplete());
    return false;
  }
  if (!source_->loop_->IsInLoopThread()) {
    result_.SetError(Errno(EINVAL));
    (void)(completion_gate_.TryComplete());
    return false;
  }
  if (source_->pending_next_ != nullptr) {
    result_.SetError(Errno(EBUSY));
    (void)(completion_gate_.TryComplete());
    return false;
  }

  if (source_->state_.State() == net::detail::RecvSourceState::kIdle) {
    if (source_->loop_->State() == backend::LoopState::kStopping ||
        source_->loop_->State() == backend::LoopState::kStopped) {
      result_.SetError(Errno(ECANCELED));
      (void)(completion_gate_.TryComplete());
      return false;
    }
    auto started = source_->Start();
    if (!started.has_value()) {
      result_.SetError(started.error());
      (void)(completion_gate_.TryComplete());
      return false;
    }
  }

  KqueueRecvSource::NextResult result;
  if (source_->TryTakeNext(result)) {
    result_.SetResult(std::move(result));
    (void)(completion_gate_.TryComplete());
    return false;
  }

  continuation_.Bind(continuation);
  source_->pending_next_ = this;
  source_->EnsureAdmission();

  // A level-triggered readiness source can already have data available when
  // admission is enabled. Recheck before parking the coroutine.
  if (source_->TryTakeNext(result)) {
    source_->pending_next_ = nullptr;
    result_.SetResult(std::move(result));
    (void)(completion_gate_.TryComplete());
    return false;
  }
  return true;
}

KqueueRecvSource::NextResult KqueueRecvSource::NextAwaiter::await_resume() noexcept {
  return result_.Take();
}

void KqueueRecvSource::NextAwaiter::Complete(NextResult result) noexcept {
  if (!completion_gate_.TryComplete()) {
    return;
  }
  result_.SetResult(std::move(result));
  continuation_.Schedule();
}

class KqueueRecvSource::StopAwaiter {
public:
  explicit StopAwaiter(KqueueRecvSource& source) noexcept : source_(&source) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (source_->pending_stop_ != nullptr) {
      result_.emplace(std::unexpected(Errno(EBUSY)));
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
      result_.emplace(Result<void>{});
      (void)(completion_gate_.TryComplete());
      return false;
    }
    return true;
  }

  Result<void> await_resume() noexcept {
    COROPACT_CHECK(result_.has_value(), "Reactor recv source Stop resumed without a result");
    return std::move(*result_);
  }

  void Complete(Result<void> result) noexcept {
    if (!completion_gate_.TryComplete()) {
      return;
    }
    result_.emplace(std::move(result));
    continuation_.Schedule();
  }

private:
  KqueueRecvSource* source_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  std::optional<Result<void>> result_;
};

Result<KqueueRecvSource> KqueueRecvSource::Create(
    KqueueLoop* loop, int fd, KqueueRecvSourceOptions options) noexcept {
  if (loop == nullptr || fd < 0 || !loop->IsInLoopThread()) {
    return std::unexpected(Errno(EINVAL));
  }
  if (!options.Valid()) {
    return std::unexpected(Errno(EINVAL));
  }
  if (options.source.buffer_capacity >
      std::numeric_limits<std::size_t>::max() / options.buffer_size) {
    return std::unexpected(Errno(EOVERFLOW));
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
    return std::unexpected(Errno(ENOMEM));
  }

  return KqueueRecvSource(loop, fd, std::move(*state), options.buffer_size, std::move(storage),
                           std::move(available_buffers));
}

KqueueRecvSource::KqueueRecvSource(KqueueLoop* loop, int fd,
                                     net::detail::RecvSourceStateMachine state,
                                     std::size_t buffer_size, std::vector<std::byte> storage,
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

KqueueRecvSource::~KqueueRecvSource() {
  if (loop_ == nullptr) {
    return;
  }

  COROPACT_CHECK(loop_->IsInLoopThread(), "KqueueRecvSource destroyed from wrong thread");
  COROPACT_CHECK(pending_next_ == nullptr, "KqueueRecvSource destroyed with pending Next");
  COROPACT_CHECK(pending_stop_ == nullptr, "KqueueRecvSource destroyed with pending Stop");
  COROPACT_CHECK(state_.State() == net::detail::RecvSourceState::kIdle ||
                     state_.State() == net::detail::RecvSourceState::kTerminal,
                 "KqueueRecvSource destroyed before Stop completed");
  COROPACT_CHECK(events_.empty(), "KqueueRecvSource destroyed with queued events");
  COROPACT_CHECK(state_.OutstandingLeases() == 0,
                 "KqueueRecvSource destroyed with outstanding leases");
  COROPACT_CHECK(available_buffers_.size() == state_.Options().buffer_capacity,
                 "KqueueRecvSource destroyed with a missing buffer");

  LoopAccess::UnregisterShutdownParticipant(*loop_, shutdown_participant_);
  DetachChannel();
}

KqueueRecvSource::KqueueRecvSource(KqueueRecvSource&& other) noexcept
    : loop_(other.loop_),
      fd_(std::exchange(other.fd_, -1)),
      state_(std::move(other.state_)),
      channel_(std::move(other.channel_)),
      events_(std::move(other.events_)),
      terminal_error_(std::move(other.terminal_error_)),
      buffer_size_(std::exchange(other.buffer_size_, 0)),
      storage_(std::move(other.storage_)),
      available_buffers_(std::move(other.available_buffers_)) {
  COROPACT_CHECK(other.pending_next_ == nullptr, "KqueueRecvSource cannot move with pending Next");
  COROPACT_CHECK(other.pending_stop_ == nullptr, "KqueueRecvSource cannot move with pending Stop");
  COROPACT_CHECK(other.state_.State() == net::detail::RecvSourceState::kIdle ||
                     other.state_.State() == net::detail::RecvSourceState::kTerminal,
                 "KqueueRecvSource cannot move while active");
  COROPACT_CHECK(other.events_.empty(), "KqueueRecvSource cannot move with queued events");
  COROPACT_CHECK(other.state_.OutstandingLeases() == 0,
                 "KqueueRecvSource cannot move with outstanding leases");
  LoopAccess::UnregisterShutdownParticipant(*other.loop_, other.shutdown_participant_);
  other.loop_ = nullptr;
  BindChannelCallbacks();
  LoopAccess::RegisterShutdownParticipant(*loop_, shutdown_participant_);
}

KqueueRecvSource& KqueueRecvSource::operator=(KqueueRecvSource&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  COROPACT_CHECK(pending_next_ == nullptr, "KqueueRecvSource destination has pending Next");
  COROPACT_CHECK(pending_stop_ == nullptr, "KqueueRecvSource destination has pending Stop");
  COROPACT_CHECK(state_.State() == net::detail::RecvSourceState::kIdle ||
                     state_.State() == net::detail::RecvSourceState::kTerminal,
                 "KqueueRecvSource destination is active");
  COROPACT_CHECK(events_.empty(), "KqueueRecvSource destination has queued events");
  COROPACT_CHECK(state_.OutstandingLeases() == 0,
                 "KqueueRecvSource destination has outstanding leases");

  COROPACT_CHECK(other.pending_next_ == nullptr, "KqueueRecvSource source has pending Next");
  COROPACT_CHECK(other.pending_stop_ == nullptr, "KqueueRecvSource source has pending Stop");
  COROPACT_CHECK(other.state_.State() == net::detail::RecvSourceState::kIdle ||
                     other.state_.State() == net::detail::RecvSourceState::kTerminal,
                 "KqueueRecvSource source is active");
  COROPACT_CHECK(other.events_.empty(), "KqueueRecvSource source has queued events");
  COROPACT_CHECK(other.state_.OutstandingLeases() == 0,
                 "KqueueRecvSource source has outstanding leases");

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

Result<void> KqueueRecvSource::Start() noexcept {
  if (state_.State() != net::detail::RecvSourceState::kIdle) {
    return std::unexpected(Errno(EALREADY));
  }

  auto started = state_.Start();
  if (!started.has_value()) {
    return started;
  }
  EnsureAdmission();
  if (state_.ArmedRequests() == 0) {
    return std::unexpected(Errno(ENOBUFS));
  }
  return {};
}

Result<bool> KqueueRecvSource::BeginStop() noexcept {
  auto stopped = state_.RequestStop();
  if (!stopped.has_value()) {
    return std::unexpected(stopped.error());
  }

  CompleteReadiness();
  DeliverNextIfReady();

  return state_.State() != net::detail::RecvSourceState::kTerminal;
}

void KqueueRecvSource::EnsureAdmission() noexcept {
  if (loop_ == nullptr || !loop_->IsInLoopThread() ||
      state_.State() != net::detail::RecvSourceState::kActive || state_.ArmedRequests() != 0) {
    return;
  }
  if (state_.TryArm()) {
    channel_.EnableReading();
  }
}

void KqueueRecvSource::RequestBackendPause() noexcept {
  auto paused = state_.RequestPause();
  COROPACT_CHECK(paused.has_value(), "KqueueRecvSource failed to enter the paused state");
  CompleteReadiness();
}

void KqueueRecvSource::CompleteReadiness() noexcept {
  if (channel_.IsReading()) {
    channel_.DisableReading();
  }
  if (state_.ArmedRequests() != 0) {
    auto completed = state_.CompleteMultishotEvent(
        net::detail::EventDisposition::kNone, net::detail::MultishotRequestDisposition::kTerminal);
    COROPACT_CHECK(completed.has_value(), "KqueueRecvSource failed to complete readiness request");
  }
}

void KqueueRecvSource::RequestBackendStop(std::optional<Error> error) noexcept {
  if (error.has_value() && !terminal_error_.has_value()) {
    terminal_error_ = *error;
  }

  auto stopped = state_.RequestStop();
  COROPACT_CHECK(stopped.has_value(), "KqueueRecvSource failed to enter stopping state");
  CompleteReadiness();
  DeliverNextIfReady();
  CompleteStopIfReady();
}

void KqueueRecvSource::OnReady() noexcept {
  if (state_.State() != net::detail::RecvSourceState::kActive || state_.ArmedRequests() == 0) {
    return;
  }

  while (state_.State() == net::detail::RecvSourceState::kActive) {
    if (!state_.CanQueueEvent() || available_buffers_.empty()) {
      RequestBackendPause();
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
        if (state_.State() == net::detail::RecvSourceState::kActive &&
            state_.ArmedRequests() != 0 && !channel_.IsReading()) {
          channel_.EnableReading();
        }
        return;
      }
      RequestBackendStop(Errno(error));
      return;
    }

    if (received == 0) {
      available_buffers_.push_back(buffer_id);
      RequestBackendStop();
      return;
    }

    auto recorded = state_.CompleteMultishotEvent(net::detail::EventDisposition::kProduced,
                                                  net::detail::MultishotRequestDisposition::kMore);
    if (!recorded.has_value()) {
      available_buffers_.push_back(buffer_id);
      RequestBackendStop(recorded.error());
      return;
    }

    net::BufferLease lease(data, static_cast<std::size_t>(received), buffer_id, this,
                           &ReclaimBuffer);
    try {
      events_.push_back(Event{.buffer = std::move(lease)});
    } catch (...) {
      // The temporary lease returns the buffer and decrements the outstanding
      // lease count before the queue reservation is rolled back.
      COROPACT_CHECK(state_.DiscardQueuedEvent(),
                     "KqueueRecvSource failed to roll back queue reservation");
      RequestBackendStop(Errno(ENOMEM));
      return;
    }

    if (pending_next_ != nullptr) {
      DeliverNextIfReady();
      return;
    }
  }
}

void KqueueRecvSource::OnClose() noexcept { RequestBackendStop(); }

void KqueueRecvSource::OnError() noexcept { RequestBackendStop(SocketError(fd_)); }

void KqueueRecvSource::DeliverNextIfReady() noexcept {
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

void KqueueRecvSource::CompleteStopIfReady() noexcept {
  if (pending_stop_ == nullptr || state_.State() != net::detail::RecvSourceState::kTerminal ||
      state_.ArmedRequests() != 0) {
    return;
  }

  auto* awaiter = std::exchange(pending_stop_, nullptr);
  awaiter->Complete(Result<void>{});
}

bool KqueueRecvSource::TryTakeNext(NextResult& result) noexcept {
  if (!events_.empty()) {
    Event event = std::move(events_.front());
    events_.pop_front();
    COROPACT_CHECK(state_.AcquireEvent(), "KqueueRecvSource queue and state became inconsistent");
    result = NextResult(std::in_place, std::move(event));
    if (state_.State() == net::detail::RecvSourceState::kPaused) {
      (void)(state_.TryResume());
    }
    EnsureAdmission();
    return true;
  }

  if (state_.State() == net::detail::RecvSourceState::kTerminal) {
    if (terminal_error_.has_value()) {
      result = std::unexpected(*terminal_error_);
    } else {
      result = NextResult(std::in_place, std::nullopt);
    }
    return true;
  }
  return false;
}

void KqueueRecvSource::ReturnBuffer(std::uint32_t buffer_id) noexcept {
  COROPACT_CHECK(loop_ != nullptr && loop_->IsInLoopThread(),
                 "KqueueRecvSource buffer returned from wrong thread");
  COROPACT_CHECK(buffer_id < state_.Options().buffer_capacity,
                 "KqueueRecvSource invalid buffer id");
  COROPACT_CHECK(available_buffers_.size() < state_.Options().buffer_capacity,
                 "KqueueRecvSource buffer returned twice");
  available_buffers_.push_back(buffer_id);
  COROPACT_CHECK(state_.ReleaseLease(), "KqueueRecvSource lease released twice");
  EnsureAdmission();
  DeliverNextIfReady();
  CompleteStopIfReady();
}

void KqueueRecvSource::ReclaimBuffer(void* context, std::uint32_t buffer_id) noexcept {
  static_cast<KqueueRecvSource*>(context)->ReturnBuffer(buffer_id);
}

void KqueueRecvSource::DetachChannel() noexcept {
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

void KqueueRecvSource::BindChannelCallbacks() noexcept {
  channel_.SetTriggerMode(TriggerMode::kOneShot);
  channel_.SetReadCallback(
      [](void* context) noexcept { static_cast<KqueueRecvSource*>(context)->OnReady(); }, this);
  channel_.SetCloseCallback(
      [](void* context) noexcept { static_cast<KqueueRecvSource*>(context)->OnClose(); }, this);
  channel_.SetErrorCallback(
      [](void* context) noexcept { static_cast<KqueueRecvSource*>(context)->OnError(); }, this);
}

void KqueueRecvSource::DispatchLoopStop(void* context) noexcept {
  static_cast<KqueueRecvSource*>(context)->RequestBackendStop();
}

Result<void> KqueueRecvSource::RequestStop() noexcept {
  if (loop_ == nullptr) {
    return {};
  }
  if (!loop_->IsInLoopThread()) {
    return std::unexpected(Errno(EINVAL));
  }
  auto waiting = BeginStop();
  if (!waiting.has_value()) {
    return std::unexpected(waiting.error());
  }
  return {};
}

coro::Task<Result<void>> KqueueRecvSource::Stop() {
  if (loop_ == nullptr) {
    co_return Result<void>{};
  }
  if (!loop_->IsInLoopThread()) {
    co_return std::unexpected(Errno(EINVAL));
  }
  if (pending_stop_ != nullptr) {
    co_return std::unexpected(Errno(EBUSY));
  }
  co_return co_await StopAwaiter(*this);
}

}  // namespace coropact::kqueue
