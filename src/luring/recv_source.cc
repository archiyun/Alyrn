// Copyright (c) 2026 Arsenova
#include "coropact/luring/recv_source.h"

#include <liburing.h>

#include <cassert>
#include <cerrno>
#include <coroutine>
#include <expected>
#include <limits>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/luring/loop.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/luring/detail/provided_buffer_pool.h"

namespace coropact::luring {

using namespace net::detail;

class LUringRecvSource::NextAwaiter {
public:
  explicit NextAwaiter(LUringRecvSource& source) noexcept : source_(&source) {}

  [[nodiscard]]
  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (source_->pending_next_ != nullptr) {
      result_.emplace(std::unexpected(base::MakeErrno(EBUSY)));
      COROPACT_IGNORE_RESULT(completion_gate_.TryComplete());
      return false;
    }

    continuation_.Bind(continuation);

    LUringRecvSource::Result result;
    if (source_->TryTakeNext(result)) {
      result_.emplace(std::move(result));
      COROPACT_IGNORE_RESULT(completion_gate_.TryComplete());
      return false;
    }

    source_->pending_next_ = this;
    return true;
  }

  LUringRecvSource::Result await_resume() noexcept {
    assert(result_.has_value());
    return std::move(*result_);
  }

  void Complete(LUringRecvSource::Result result) noexcept {
    if (!completion_gate_.TryComplete()) {
      return;
    }
    result_.emplace(std::move(result));
    continuation_.Schedule();
  }

private:
  LUringRecvSource* source_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  std::optional<LUringRecvSource::Result> result_;
};

class LUringRecvSource::StopAwaiter {
public:
  explicit StopAwaiter(LUringRecvSource& source) noexcept : source_(&source) {}

  [[nodiscard]]
  bool await_ready() const noexcept { return false; }

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
  LUringRecvSource* source_;
  operation::detail::SchedulerContinuation continuation_;
  operation::detail::CompletionGate completion_gate_;
  std::optional<base::Result<void>> result_;
};

base::Result<LUringRecvSource> LUringRecvSource::Create(
    LUringLoop* loop,
    int fd,
    LUringRecvSourceOptions options) noexcept {
  if (loop == nullptr || !loop->Initialized()) {
    return std::unexpected(base::MakeErrno(EBADF));
  }
  if (!loop->IsInLoopThread() || fd < 0) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (!options.Valid()) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (!loop->HasCapability(NativeFeature::kMultishotRecv) ||
      !loop->HasCapability(NativeFeature::kProvidedBufferRing)) {
    return std::unexpected(base::MakeErrno(ENOTSUP));
  }

  const std::size_t capacity = options.source.buffer_capacity;
  if (capacity > std::numeric_limits<std::size_t>::max() / options.buffer_size) {
    return std::unexpected(base::MakeErrno(EOVERFLOW));
  }

  auto state_result = RecvSourceStateMachine::Create(options.source);
  if (!state_result.has_value()) {
    return std::unexpected(state_result.error());
  }

  auto shared_pool = loop->GetSharedProvidedBufferPool(
      options.buffer_size, loop->shared_buffer_storage_);
  if (!shared_pool.has_value()) {
    if (shared_pool.error().value() == ENOENT) {
      return std::unexpected(base::MakeErrno(ENOTSUP));
    }
    return std::unexpected(shared_pool.error());
  }
  if (capacity > (*shared_pool)->capacity()) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }

  return LUringRecvSource(
      loop,
      fd,
      std::move(*state_result),
      options.buffer_size,
      *shared_pool);
}

LUringRecvSource::LUringRecvSource(
    LUringLoop* loop,
    int fd,
    RecvSourceStateMachine state,
    std::size_t buffer_size,
    detail::ProvidedBufferPool* shared_buffer_pool) noexcept
    : loop_(loop),
      fd_(fd),
      state_(std::move(state)),
      recv_op_(this),
      cancel_op_(this),
      buffer_size_(buffer_size),
      shared_buffer_pool_(shared_buffer_pool) {}

LUringRecvSource::~LUringRecvSource() {
  if (loop_ == nullptr) {
    return;
  }

  COROPACT_DCHECK(loop_->IsInLoopThread(),
                 "LUringRecvSource destroyed from wrong thread");
  COROPACT_DCHECK(pending_next_ == nullptr,
                 "LUringRecvSource destroyed with pending Next");
  COROPACT_DCHECK(pending_stop_ == nullptr,
                 "LUringRecvSource destroyed with pending Stop");
  COROPACT_DCHECK(!recv_submitted_,
                 "LUringRecvSource destroyed with active recv");
  COROPACT_DCHECK(!cancel_submitted_,
                 "LUringRecvSource destroyed with active cancel");
  COROPACT_DCHECK(state_.State() == RecvSourceState::kIdle ||
                     state_.State() == RecvSourceState::kTerminal,
                 "LUringRecvSource destroyed before Stop completed");
  COROPACT_DCHECK(events_.empty(),
                 "LUringRecvSource destroyed with queued events");
  COROPACT_DCHECK(state_.OutstandingLeases() == 0,
                 "LUringRecvSource destroyed with outstanding leases");
}

LUringRecvSource::LUringRecvSource(LUringRecvSource&& other) noexcept
    : loop_(std::exchange(other.loop_, nullptr)),
      fd_(std::exchange(other.fd_, -1)),
      state_(std::move(other.state_)),
      events_(std::move(other.events_)),
      terminal_error_(std::move(other.terminal_error_)),
      recv_op_(this),
      cancel_op_(this),
      buffer_size_(std::exchange(other.buffer_size_, 0)),
      shared_buffer_pool_(std::exchange(other.shared_buffer_pool_, nullptr)),
      recv_submitted_(false),
      cancel_submitted_(false) {
  COROPACT_CHECK(other.pending_next_ == nullptr,
                 "LUringRecvSource cannot move with pending Next");
  COROPACT_CHECK(other.pending_stop_ == nullptr,
                 "LUringRecvSource cannot move with pending Stop");
  COROPACT_CHECK(!other.recv_submitted_,
                 "LUringRecvSource cannot move while active");
  COROPACT_CHECK(!other.cancel_submitted_,
                 "LUringRecvSource cannot move while cancelling");
  COROPACT_CHECK(other.state_.OutstandingLeases() == 0,
                 "LUringRecvSource cannot move with outstanding leases");
  COROPACT_CHECK(other.state_.State() == RecvSourceState::kIdle ||
                     other.state_.State() == RecvSourceState::kTerminal,
                 "LUringRecvSource cannot move before Stop completed");
  COROPACT_CHECK(other.events_.empty(),
                 "LUringRecvSource cannot move with queued events");
}

LUringRecvSource& LUringRecvSource::operator=(LUringRecvSource&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  COROPACT_CHECK(pending_next_ == nullptr,
                 "LUringRecvSource destination has pending Next");
  COROPACT_CHECK(pending_stop_ == nullptr,
                 "LUringRecvSource destination has pending Stop");
  COROPACT_CHECK(!recv_submitted_,
                 "LUringRecvSource destination is active");
  COROPACT_CHECK(!cancel_submitted_,
                 "LUringRecvSource destination is cancelling");
  COROPACT_CHECK(state_.OutstandingLeases() == 0,
                 "LUringRecvSource destination has outstanding leases");
  COROPACT_CHECK(events_.empty(),
                 "LUringRecvSource destination has queued events");

  COROPACT_CHECK(other.pending_next_ == nullptr,
                 "LUringRecvSource source has pending Next");
  COROPACT_CHECK(other.pending_stop_ == nullptr,
                 "LUringRecvSource source has pending Stop");
  COROPACT_CHECK(!other.recv_submitted_,
                 "LUringRecvSource source is active");
  COROPACT_CHECK(!other.cancel_submitted_,
                 "LUringRecvSource source is cancelling");
  COROPACT_CHECK(other.state_.OutstandingLeases() == 0,
                 "LUringRecvSource source has outstanding leases");
  COROPACT_CHECK(other.state_.State() == RecvSourceState::kIdle ||
                     other.state_.State() == RecvSourceState::kTerminal,
                 "LUringRecvSource source has not completed Stop");
  COROPACT_CHECK(other.events_.empty(),
                 "LUringRecvSource source has queued events");

  loop_ = std::exchange(other.loop_, nullptr);
  fd_ = std::exchange(other.fd_, -1);
  state_ = std::move(other.state_);
  terminal_error_ = std::move(other.terminal_error_);
  buffer_size_ = std::exchange(other.buffer_size_, 0);
  shared_buffer_pool_ = std::exchange(other.shared_buffer_pool_, nullptr);
  return *this;
}

base::Result<void> LUringRecvSource::StartOperation() noexcept {
  if (loop_ == nullptr || !loop_->Initialized() || fd_ < 0 ||
      shared_buffer_pool_ == nullptr) {
    return std::unexpected(base::MakeErrno(EBADF));
  }
  if (recv_submitted_) {
    return {};
  }
  if (!state_.CanArm() || !state_.TryArm()) {
    return {};
  }

  recv_op_.Prepare();
  const auto buffer_group = shared_buffer_pool_->BufferGroup();
  auto submitted = loop_->SubmitOp(
      &recv_op_,
      [fd = fd_, buffer_size = buffer_size_, buffer_group](
          io_uring_sqe* sqe) noexcept {
        io_uring_prep_recv_multishot(sqe, fd, nullptr, buffer_size, 0);
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = buffer_group;
      });

  if (!submitted.has_value()) {
    auto completed = state_.CompleteMultishotEvent(
        EventDisposition::kNone, MultishotRequestDisposition::kTerminal);
    assert(completed.has_value());
    return std::unexpected(submitted.error());
  }

  recv_submitted_ = true;
  return {};
}

base::Result<void> LUringRecvSource::Start() noexcept {
  if (state_.State() != RecvSourceState::kIdle) {
    return std::unexpected(base::MakeErrno(EALREADY));
  }
  if (loop_ == nullptr || !loop_->Initialized() || fd_ < 0) {
    return std::unexpected(base::MakeErrno(EBADF));
  }

  auto started = state_.Start();
  if (!started.has_value()) {
    return started;
  }

  auto submitted = StartOperation();
  if (!submitted.has_value()) {
    RequestBackendStop(submitted.error());
    return std::unexpected(submitted.error());
  }
  return {};
}

base::Result<void> LUringRecvSource::StartCancel() noexcept {
  if (!recv_submitted_ || cancel_submitted_) {
    return {};
  }

  cancel_op_.Prepare();
  const auto target = reinterpret_cast<std::uint64_t>(&recv_op_);
  auto submitted = loop_->SubmitOp(
      &cancel_op_, [target](io_uring_sqe* sqe) noexcept {
        io_uring_prep_cancel64(sqe, target, IORING_ASYNC_CANCEL_ALL);
      });
  if (!submitted.has_value()) {
    return std::unexpected(submitted.error());
  }

  cancel_submitted_ = true;
  return {};
}

base::Result<bool> LUringRecvSource::BeginStop() noexcept {
  auto stopped = state_.RequestStop();
  if (!stopped.has_value()) {
    return std::unexpected(stopped.error());
  }

  if (recv_submitted_) {
    auto cancelled = StartCancel();
    if (!cancelled.has_value()) {
      return std::unexpected(cancelled.error());
    }
  }

  return state_.State() != RecvSourceState::kTerminal ||
         recv_submitted_ || cancel_submitted_;
}

void LUringRecvSource::EnsureSubmission() noexcept {
  if (loop_ == nullptr || !loop_->Initialized() ||
      state_.State() != RecvSourceState::kActive ||
      recv_submitted_ || cancel_submitted_) {
    return;
  }

  auto submitted = StartOperation();
  if (!submitted.has_value()) {
    RequestBackendStop(submitted.error());
    DeliverNextIfReady();
  }
}

void LUringRecvSource::RequestBackendPause() noexcept {
  auto paused = state_.RequestPause();
  assert(paused.has_value());

  if (recv_submitted_ && !cancel_submitted_) {
    auto cancelled = StartCancel();
    if (!cancelled.has_value()) {
      RequestBackendStop(cancelled.error());
    }
  }
}

void LUringRecvSource::MaybeResume() noexcept {
  if (terminal_error_.has_value() || loop_ == nullptr || !loop_->Initialized() ||
      cancel_submitted_) {
    return;
  }

  if (state_.TryResume()) {
    EnsureSubmission();
  }
}

void LUringRecvSource::RequestBackendStop(std::optional<base::Error> error) noexcept {
  if (error.has_value() && !terminal_error_.has_value()) {
    terminal_error_ = *error;
  }

  auto stopped = state_.RequestStop();
  assert(stopped.has_value());

  if (recv_submitted_ && !cancel_submitted_) {
    auto cancelled = StartCancel();
    if (!cancelled.has_value() && !terminal_error_.has_value()) {
      terminal_error_ = cancelled.error();
    }
  }
}

CompletionDisposition LUringRecvSource::OnCompletion(CompletionEvent event) noexcept {
  const bool request_still_active = event.More();
  const int cqe_result = event.result;

  if (!request_still_active) {
    recv_submitted_ = false;
  }

  const bool has_buffer = (event.flags & IORING_CQE_F_BUFFER) != 0;
  const std::uint32_t buffer_id =
      static_cast<std::uint32_t>(event.flags >> IORING_CQE_BUFFER_SHIFT);
  const bool valid_buffer = has_buffer && shared_buffer_pool_ != nullptr &&
                            buffer_id < shared_buffer_pool_->capacity();

  bool state_recorded = false;
  bool buffer_prepared = false;
  std::optional<base::Error> completion_error;

  if (event.BufferMore()) {
    completion_error = base::MakeErrno(EPROTO);
  }

  if (cqe_result > 0 && completion_error.has_value() == false) {
    if (!valid_buffer) {
      completion_error = base::MakeErrno(EPROTO);
    } else {
      if (!shared_buffer_pool_->Acquire(buffer_id)) {
        completion_error = base::MakeErrno(EPROTO);
      } else {
        buffer_prepared = true;
        if (static_cast<std::size_t>(cqe_result) > buffer_size_) {
          completion_error = base::MakeErrno(EOVERFLOW);
        }
      }
    }
  }

  if (cqe_result > 0) {
    if (completion_error.has_value()) {
      if (buffer_prepared) {
        COROPACT_CHECK(shared_buffer_pool_->Return(buffer_id),
                       "LUringRecvSource failed to return shared buffer");
      }
      RequestBackendStop(*completion_error);
    } else if (!state_.CanQueueEvent()) {
      if (buffer_prepared) {
        COROPACT_CHECK(shared_buffer_pool_->Return(buffer_id),
                       "LUringRecvSource failed to return shared buffer");
      }
      RequestBackendPause();
    } else {
      auto recorded = state_.CompleteMultishotEvent(
          EventDisposition::kProduced,
          request_still_active ? MultishotRequestDisposition::kMore
                               : MultishotRequestDisposition::kTerminal);
      if (!recorded.has_value()) {
        // The failed accounting attempt did not consume the state-machine
        // event; the fallback below will record its terminal/non-terminal
        // request completion.
        COROPACT_CHECK(shared_buffer_pool_->Return(buffer_id),
                       "LUringRecvSource failed to return shared buffer");
        RequestBackendStop(recorded.error());
      } else {
        state_recorded = true;
        net::BufferLease lease(
            shared_buffer_pool_->slot(buffer_id),
            static_cast<std::size_t>(cqe_result),
            buffer_id,
            this,
            &ReclaimBuffer);
        try {
          events_.push_back(Event{.buffer = std::move(lease)});
        } catch (...) {
          // The temporary Event owns the lease while deque insertion is in
          // progress. Its destruction returns the buffer and decrements the
          // outstanding-buffer count; then remove the queue reservation.
          assert(state_.DiscardQueuedEvent());
          RequestBackendStop(base::MakeErrno(ENOMEM));
        }
      }
    }
  } else if (cqe_result == 0) {
    if (valid_buffer && !event.BufferMore() &&
        shared_buffer_pool_->Acquire(buffer_id)) {
      COROPACT_CHECK(shared_buffer_pool_->Return(buffer_id),
                     "LUringRecvSource failed to return shared buffer");
    }
    if (!request_still_active) {
      RequestBackendStop();
    }
  } else {
    if (valid_buffer && !event.BufferMore() &&
        shared_buffer_pool_->Acquire(buffer_id)) {
      COROPACT_CHECK(shared_buffer_pool_->Return(buffer_id),
                     "LUringRecvSource failed to return shared buffer");
    }
    const auto state = state_.State();
    const bool stopping = state == RecvSourceState::kStopping ||
                          state == RecvSourceState::kPausing ||
                          state == RecvSourceState::kPaused ||
                          state == RecvSourceState::kDraining ||
                          state == RecvSourceState::kTerminal;
    if (!stopping) {
      RequestBackendStop(base::MakeNegErrno(cqe_result));
    }
  }

  if (!state_recorded) {
    auto recorded = state_.CompleteMultishotEvent(
        EventDisposition::kNone,
        request_still_active ? MultishotRequestDisposition::kMore
                             : MultishotRequestDisposition::kTerminal);
    if (!recorded.has_value()) {
      terminal_error_ = recorded.error();
      RequestBackendStop(recorded.error());
    }
  }

  if (!request_still_active) {
    recv_op_.BeginNextRequest();
  }

  if (state_.State() == RecvSourceState::kActive &&
      state_.QueuedEvents() >= state_.Options().event_capacity) {
    RequestBackendPause();
  }

  if (!request_still_active && state_.State() == RecvSourceState::kActive &&
      !terminal_error_.has_value()) {
    EnsureSubmission();
  }

  DeliverNextIfReady();
  MaybeResume();
  CompleteStopIfReady();

  return CompletionDisposition{
      .kernel_request_terminal = !request_still_active,
      .decrement_inflight = !request_still_active,
      .resume_continuation = false,
  };
}

void LUringRecvSource::OnCancelComplete(int cqe_result) noexcept {
  cancel_submitted_ = false;
  if (cqe_result < 0 && cqe_result != -ENOENT && cqe_result != -ECANCELED &&
      !terminal_error_.has_value()) {
    terminal_error_ = base::MakeNegErrno(cqe_result);
  }
  MaybeResume();
  CompleteStopIfReady();
}

void LUringRecvSource::DeliverNextIfReady() noexcept {
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

void LUringRecvSource::CompleteStopIfReady() noexcept {
  if (pending_stop_ == nullptr || recv_submitted_ || cancel_submitted_ ||
      state_.State() != RecvSourceState::kTerminal) {
    return;
  }

  auto* awaiter = std::exchange(pending_stop_, nullptr);
  awaiter->Complete(base::Result<void>{});
}

bool LUringRecvSource::TryTakeNext(Result& result) noexcept {
  if (!events_.empty()) {
    Event event = std::move(events_.front());
    events_.pop_front();
    COROPACT_CHECK(state_.AcquireEvent(),
                   "LUringRecvSource: queue and state became inconsistent");
    result = Result(std::in_place, std::move(event));
    if (state_.State() == RecvSourceState::kPaused) {
      MaybeResume();
    }
    return true;
  }

  if (state_.State() == RecvSourceState::kTerminal) {
    if (terminal_error_.has_value()) {
      result = std::unexpected(*terminal_error_);
    } else {
      result = Result(std::in_place, std::nullopt);
    }
    return true;
  }
  return false;
}

void LUringRecvSource::ReturnBuffer(std::uint32_t buffer_id) noexcept {
  COROPACT_CHECK(shared_buffer_pool_ != nullptr,
                 "LUringRecvSource missing shared buffer pool");
  COROPACT_CHECK(shared_buffer_pool_->Return(buffer_id),
                 "LUringRecvSource shared buffer lease released twice");
  COROPACT_CHECK(state_.ReleaseLease(),
                 "LUringRecvSource buffer lease released twice");
  EnsureSubmission();
  CompleteStopIfReady();
}

void LUringRecvSource::ReclaimBuffer(
    void* context,
    std::uint32_t buffer_id) noexcept {
  static_cast<LUringRecvSource*>(context)->ReturnBuffer(buffer_id);
}

coro::Task<LUringRecvSource::Result> LUringRecvSource::Next() {
  if (loop_ == nullptr || fd_ < 0) {
    co_return std::unexpected(base::MakeErrno(EBADF));
  }
  if (!loop_->IsInLoopThread()) {
    co_return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (pending_next_ != nullptr) {
    co_return std::unexpected(base::MakeErrno(EBUSY));
  }

  if (state_.State() == RecvSourceState::kIdle) {
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

base::Result<void> LUringRecvSource::RequestStop() noexcept {
  if (loop_ == nullptr || fd_ < 0) {
    return std::unexpected(base::MakeErrno(EBADF));
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

coro::Task<base::Result<void>> LUringRecvSource::Stop() {
  if (loop_ == nullptr) {
    co_return base::Result<void>{};
  }
  if (!loop_->IsInLoopThread()) {
    co_return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (pending_stop_ != nullptr) {
    co_return std::unexpected(base::MakeErrno(EBUSY));
  }

  if (state_.State() == RecvSourceState::kTerminal &&
      !recv_submitted_ && !cancel_submitted_) {
    co_return base::Result<void>{};
  }

  co_return co_await StopAwaiter(*this);
}

namespace detail {

CompletionDisposition DispatchRecvSourceComplete(
    LUringOp* op,
    CompletionEvent event) noexcept {
  auto* operation = static_cast<LUringRecvSource::RecvOperation*>(op);
  return operation->Source()->OnCompletion(event);
}

void DispatchRecvSourceCancelComplete(LUringOp* op) noexcept {
  auto* operation = static_cast<LUringRecvSource::CancelOperation*>(op);

  int result = -EIO;
  if (op->result.HasValue()) {
    result = *op->result;
  }
  operation->Source()->OnCancelComplete(result);
}

}  // namespace detail

}  // namespace coropact::luring
