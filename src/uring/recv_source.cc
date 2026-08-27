#include "alyrn/uring/recv_source.h"

#include <cerrno>
#include <cstddef>
#include <coroutine>
#include <cstdint>
#include <expected>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "alyrn/detail/base/check.h"
#include "alyrn/detail/uring/cancel_result.h"
#include "alyrn/detail/uring/completion_dispatch.h"
#include "alyrn/detail/uring/loop_access.h"
#include "alyrn/detail/uring/op.h"
#include "alyrn/detail/uring/provided_buffer_pool.h"
#include "alyrn/detail/uring/sqe_prep.h"
#include "alyrn/uring/loop.h"
#include "alyrn/detail/net/recv_source_state.h"
#include "alyrn/detail/net/source_state.h"
#include "alyrn/net/recv_source.h"
#include "alyrn/detail/operation/completion_gate.h"
#include "alyrn/detail/operation/scheduler_continuation.h"
#include "alyrn/result.h"

namespace alyrn::uring {

using namespace detail;
using namespace net::detail;

bool RecvSource::NextAwaiter::await_suspend(std::coroutine_handle<> continuation) noexcept {
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

  if (source_->state_.State() == RecvSourceState::kIdle) {
    auto started = source_->Start();
    if (!started.has_value()) {
      result_.SetError(started.error());
      (void)(completion_gate_.TryComplete());
      return false;
    }
  }

  RecvSource::NextResult result;
  if (source_->TryTakeNext(result)) {
    result_.SetResult(std::move(result));
    (void)(completion_gate_.TryComplete());
    return false;
  }

  resume_work_.SetHandle(continuation);
  source_->pending_next_ = this;
  return true;
}

RecvSource::NextResult RecvSource::NextAwaiter::await_resume() noexcept {
  return result_.Take();
}

void RecvSource::NextAwaiter::Complete(NextResult result) noexcept {
  if (!completion_gate_.TryComplete()) {
    return;
  }
  result_.SetResult(std::move(result));
  detail::LoopAccess::ScheduleCompletion(*source_->loop_, &resume_work_);
}

class RecvSource::StopAwaiter {
public:
  explicit StopAwaiter(RecvSource& source) noexcept : source_(&source) {}

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
    ALYRN_CHECK(result_.has_value(), "Uring recv source Stop resumed without a result");
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
  RecvSource* source_;
  ::alyrn::detail::operation::SchedulerContinuation continuation_;
  ::alyrn::detail::operation::CompletionGate completion_gate_;
  std::optional<Result<void>> result_;
};

Result<RecvSource> RecvSource::Create(Loop* loop, int fd,
                                                  RecvSourceOptions options) noexcept {
  if (loop == nullptr || !loop->Initialized()) {
    return std::unexpected(Errno(EBADF));
  }
  if (!loop->IsInLoopThread() || fd < 0) {
    return std::unexpected(Errno(EINVAL));
  }
  if (!options.Valid()) {
    return std::unexpected(Errno(EINVAL));
  }
  const std::size_t capacity = options.source.buffer_capacity;
  if (capacity > std::numeric_limits<std::size_t>::max() / options.buffer_size) {
    return std::unexpected(Errno(EOVERFLOW));
  }

  auto state_result = RecvSourceStateMachine::Create(options.source);
  if (!state_result.has_value()) {
    return std::unexpected(state_result.error());
  }

  auto shared_pool = detail::LoopAccess::GetSharedProvidedBufferPool(
      *loop, options.buffer_size, options.source.buffer_capacity);
  if (!shared_pool.has_value()) {
    if (shared_pool.error().value() == ENOENT) {
      return std::unexpected(Errno(ENOTSUP));
    }
    return std::unexpected(shared_pool.error());
  }
  detail::ProvidedBufferPool* const buffer_pool = *shared_pool;
  ALYRN_CHECK(buffer_pool != nullptr, "RecvSource failed to select a buffer pool");
  if (capacity > buffer_pool->capacity()) return std::unexpected(Errno(EINVAL));

  std::vector<RecvSource::PendingEvent> event_storage;
  std::vector<RecvSource::SlotState> slot_storage;
  try {
    event_storage.resize(options.source.event_capacity);
    slot_storage.resize(buffer_pool->capacity());
  } catch (...) {
    return std::unexpected(Errno(ENOMEM));
  }

  return RecvSource(loop, fd, std::move(*state_result), options.buffer_size, buffer_pool,
                          std::move(event_storage), std::move(slot_storage));
}

RecvSource::RecvSource(
    Loop* loop, int fd, RecvSourceStateMachine state, std::size_t buffer_size,
    detail::ProvidedBufferPool* buffer_pool, std::vector<PendingEvent> event_storage,
    std::vector<SlotState> slot_storage) noexcept
    : loop_(loop),
      fd_(fd),
      state_(std::move(state)),
      events_(std::move(event_storage)),
      slots_(std::move(slot_storage)),
      recv_op_(this),
      cancel_op_(this),
      buffer_size_(buffer_size),
      buffer_pool_(buffer_pool) {}

RecvSource::~RecvSource() {
  if (loop_ == nullptr) {
    return;
  }

  ALYRN_CHECK(loop_->IsInLoopThread(), "RecvSource destroyed from wrong thread");
  ALYRN_CHECK(pending_next_ == nullptr, "RecvSource destroyed with pending Next");
  ALYRN_CHECK(pending_stop_ == nullptr, "RecvSource destroyed with pending Stop");
  ALYRN_CHECK(!recv_submitted_, "RecvSource destroyed with active recv");
  ALYRN_CHECK(!cancel_submitted_, "RecvSource destroyed with active cancel");
  ALYRN_CHECK(
      state_.State() == RecvSourceState::kIdle || state_.State() == RecvSourceState::kTerminal,
      "RecvSource destroyed before Stop completed");
  ALYRN_CHECK(event_count_ == 0, "RecvSource destroyed with queued events");
  ALYRN_CHECK(state_.OutstandingLeases() == 0,
                 "RecvSource destroyed with outstanding leases");
  for (const SlotState& slot : slots_) {
    ALYRN_CHECK(!slot.active, "RecvSource destroyed with borrowed provided buffer");
  }
}

void RecvSource::ValidateMovable(const RecvSource& source) noexcept {
  if (source.loop_ == nullptr) {
    return;
  }

  ALYRN_CHECK(source.loop_->IsInLoopThread(), "RecvSource moved from wrong thread");
  ALYRN_CHECK(source.pending_next_ == nullptr, "RecvSource cannot move with pending Next");
  ALYRN_CHECK(source.pending_stop_ == nullptr, "RecvSource cannot move with pending Stop");
  ALYRN_CHECK(!source.recv_submitted_, "RecvSource cannot move while active");
  ALYRN_CHECK(!source.cancel_submitted_, "RecvSource cannot move while cancelling");
  ALYRN_CHECK(source.state_.OutstandingLeases() == 0,
                 "RecvSource cannot move with outstanding leases");
  ALYRN_CHECK(source.state_.State() == RecvSourceState::kIdle ||
                     source.state_.State() == RecvSourceState::kTerminal,
                 "RecvSource cannot move before Stop completed");
  ALYRN_CHECK(source.event_count_ == 0, "RecvSource cannot move with queued events");
  for (const SlotState& slot : source.slots_) {
    ALYRN_CHECK(!slot.active, "RecvSource cannot move with borrowed provided buffer");
  }
}

RecvSource::RecvSource(RecvSource&& other) noexcept
    : loop_((ValidateMovable(other), std::exchange(other.loop_, nullptr))),
      fd_(std::exchange(other.fd_, -1)),
      state_(std::move(other.state_)),
      events_(std::move(other.events_)),
      slots_(std::move(other.slots_)),
      event_head_(std::exchange(other.event_head_, 0)),
      event_count_(std::exchange(other.event_count_, 0)),
      terminal_error_(std::move(other.terminal_error_)),
      recv_op_(this),
      cancel_op_(this),
      buffer_size_(std::exchange(other.buffer_size_, 0)),
      buffer_pool_(std::exchange(other.buffer_pool_, nullptr)),
      recv_submitted_(false),
      cancel_submitted_(false) {}

RecvSource& RecvSource::operator=(RecvSource&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  ValidateMovable(*this);
  ValidateMovable(other);
  ALYRN_CHECK(loop_ == nullptr || other.loop_ == nullptr || loop_ == other.loop_,
                 "RecvSource cannot move across loops");

  loop_ = std::exchange(other.loop_, nullptr);
  fd_ = std::exchange(other.fd_, -1);
  state_ = std::move(other.state_);
  events_ = std::move(other.events_);
  slots_ = std::move(other.slots_);
  event_head_ = std::exchange(other.event_head_, 0);
  event_count_ = std::exchange(other.event_count_, 0);
  terminal_error_ = std::move(other.terminal_error_);
  buffer_size_ = std::exchange(other.buffer_size_, 0);
  buffer_pool_ = std::exchange(other.buffer_pool_, nullptr);
  return *this;
}

Result<void> RecvSource::StartOperation() noexcept {
  if (loop_ == nullptr || !loop_->Initialized() || fd_ < 0 || buffer_pool_ == nullptr) {
    return std::unexpected(Errno(EBADF));
  }
  if (recv_submitted_) {
    return {};
  }
  if (!state_.CanArm() || !state_.TryArm()) {
    return {};
  }

  recv_op_.Prepare();
  const auto buffer_group = buffer_pool_->BufferGroup();
  auto submitted = detail::LoopAccess::SubmitOp(
      *loop_, &recv_op_, detail::PrepareProvidedRecvMultishot(fd_, buffer_size_, buffer_group));

  if (!submitted.has_value()) {
    const auto completed = state_.CompleteMultishotEvent(EventDisposition::kNone,
                                                         MultishotRequestDisposition::kTerminal);
    (void)(completed);
    ALYRN_CHECK(completed.has_value(),
                   "Uring recv source failed to record terminal submit failure");
    return std::unexpected(submitted.error());
  }

  recv_submitted_ = true;
  return {};
}

Result<void> RecvSource::Start() noexcept {
  if (state_.State() != RecvSourceState::kIdle) {
    return std::unexpected(Errno(EALREADY));
  }
  if (loop_ == nullptr || !loop_->Initialized() || fd_ < 0) {
    return std::unexpected(Errno(EBADF));
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

Result<void> RecvSource::StartCancel() noexcept {
  if (!recv_submitted_ || cancel_submitted_) {
    return {};
  }

  cancel_op_.Prepare();
  const auto target = reinterpret_cast<std::uint64_t>(&recv_op_);
  auto submitted = detail::LoopAccess::SubmitOp(
      *loop_, &cancel_op_, detail::PrepareCancelAllByUserData(target));
  if (!submitted.has_value()) {
    return std::unexpected(submitted.error());
  }

  cancel_submitted_ = true;
  return {};
}

Result<bool> RecvSource::BeginStop() noexcept {
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

  return state_.State() != RecvSourceState::kTerminal || recv_submitted_ || cancel_submitted_;
}

void RecvSource::EnsureSubmission() noexcept {
  if (loop_ == nullptr || !loop_->Initialized() || state_.State() != RecvSourceState::kActive ||
      recv_submitted_ || cancel_submitted_) {
    return;
  }

  auto submitted = StartOperation();
  if (!submitted.has_value()) {
    RequestBackendStop(submitted.error());
    DeliverNextIfReady();
  }
}

void RecvSource::RequestBackendPause() noexcept {
  (void)(state_.RequestPause());

  if (recv_submitted_ && !cancel_submitted_) {
    auto cancelled = StartCancel();
    if (!cancelled.has_value()) {
      RequestBackendStop(cancelled.error());
    }
  }
}

void RecvSource::MaybeResume() noexcept {
  if (terminal_error_.has_value() || loop_ == nullptr || !loop_->Initialized() ||
      cancel_submitted_) {
    return;
  }

  if (state_.TryResume()) {
    EnsureSubmission();
  }
}

void RecvSource::RequestBackendStop(std::optional<Error> error) noexcept {
  if (error.has_value() && !terminal_error_.has_value()) {
    terminal_error_ = *error;
  }

  (void)(state_.RequestStop());

  if (recv_submitted_ && !cancel_submitted_) {
    auto cancelled = StartCancel();
    if (!cancelled.has_value() && !terminal_error_.has_value()) {
      terminal_error_ = cancelled.error();
    }
  }
}

CompletionDisposition RecvSource::OnCompletion(CompletionEvent event) noexcept {
  const bool request_still_active = event.More();
  const int cqe_result = event.result;

  if (!request_still_active) {
    recv_submitted_ = false;
  }

  const bool has_buffer = event.HasSelectedBuffer();
  const auto buffer_id = event.SelectedBufferId();
  const bool valid_buffer =
      has_buffer && buffer_pool_ != nullptr && buffer_id < buffer_pool_->capacity();

  bool state_recorded = false;
  std::optional<Error> completion_error;

  if (cqe_result > 0) {
    if (!valid_buffer || event.BufferMore()) {
      completion_error = Errno(EPROTO);
    } else {
      auto acquired = AcquireBuffer(buffer_id, static_cast<std::size_t>(cqe_result));
      if (!acquired.has_value()) {
        completion_error = acquired.error();
      } else {
        if (!event.BufferMore()) {
          MarkKernelDone(buffer_id);
        }

        const bool direct_delivery = pending_next_ != nullptr && event_count_ == 0 &&
                                     state_.State() == RecvSourceState::kActive;
        if (!direct_delivery && !state_.CanQueueEvent()) {
          ReleaseSlotLease(buffer_id);
          RequestBackendPause();
        } else {
          auto recorded = state_.CompleteMultishotEvent(
              direct_delivery ? EventDisposition::kDelivered : EventDisposition::kProduced,
              request_still_active ? MultishotRequestDisposition::kMore
                                   : MultishotRequestDisposition::kTerminal);
          if (!recorded.has_value()) {
            ReleaseSlotLease(buffer_id);
            RequestBackendStop(recorded.error());
          } else {
            state_recorded = true;
            if (direct_delivery) {
              net::BufferLease lease = MakeLease(buffer_id, static_cast<std::size_t>(cqe_result));
              auto* awaiter = std::exchange(pending_next_, nullptr);
              awaiter->Complete(NextResult(std::in_place, Event{.buffer = std::move(lease)}));
            } else {
              QueueEvent(buffer_id, static_cast<std::size_t>(cqe_result));
            }
          }
        }
      }
    }
  } else if (cqe_result == 0) {
    if (event.BufferMore()) {
      completion_error = Errno(EPROTO);
    } else if (valid_buffer && slots_[buffer_id].active) {
      MarkKernelDone(buffer_id);
    } else if (valid_buffer && buffer_pool_->Acquire(buffer_id)) {
      ALYRN_CHECK(buffer_pool_->Return(buffer_id),
                     "RecvSource failed to return empty provided buffer");
    }
    if (!request_still_active) {
      RequestBackendStop();
    }
  } else {
    if (event.BufferMore()) {
      completion_error = Errno(EPROTO);
    } else if (valid_buffer && slots_[buffer_id].active) {
      MarkKernelDone(buffer_id);
    } else if (valid_buffer && buffer_pool_->Acquire(buffer_id)) {
      ALYRN_CHECK(buffer_pool_->Return(buffer_id),
                     "RecvSource failed to return error provided buffer");
    }
    const auto state = state_.State();
    const bool stopping = state == RecvSourceState::kStopping ||
                          state == RecvSourceState::kPausing || state == RecvSourceState::kPaused ||
                          state == RecvSourceState::kDraining ||
                          state == RecvSourceState::kTerminal;
    if (!stopping) {
      RequestBackendStop(NegErrno(cqe_result));
    }
  }

  if (completion_error.has_value()) {
    RequestBackendStop(*completion_error);
  }

  if (!state_recorded) {
    auto recorded = state_.CompleteMultishotEvent(
        EventDisposition::kNone, request_still_active ? MultishotRequestDisposition::kMore
                                                      : MultishotRequestDisposition::kTerminal);
    if (!recorded.has_value()) {
      terminal_error_ = recorded.error();
      RequestBackendStop(recorded.error());
    }
  }

  if (!request_still_active) {
    MarkActiveSlotsKernelDone();
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

void RecvSource::OnCancelComplete(int cqe_result) noexcept {
  cancel_submitted_ = false;
  if (!detail::IsExpectedCancelCqeResult(cqe_result) && !terminal_error_.has_value()) {
    terminal_error_ = NegErrno(cqe_result);
  }
  MaybeResume();
  CompleteStopIfReady();
}

void RecvSource::DeliverNextIfReady() noexcept {
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

void RecvSource::CompleteStopIfReady() noexcept {
  if (pending_stop_ == nullptr || recv_submitted_ || cancel_submitted_ ||
      state_.State() != RecvSourceState::kTerminal) {
    return;
  }

  auto* awaiter = std::exchange(pending_stop_, nullptr);
  awaiter->Complete(Result<void>{});
}

bool RecvSource::TryTakeNext(NextResult& result) noexcept {
  PendingEvent pending;
  if (TryTakeQueuedEvent(pending)) {
    ALYRN_CHECK(state_.AcquireEvent(), "RecvSource: queue and state became inconsistent");
    Event event{.buffer = MakeLease(pending.buffer_id, pending.size)};
    result = NextResult(std::in_place, std::move(event));
    if (state_.State() == RecvSourceState::kPaused) {
      MaybeResume();
    }
    return true;
  }

  if (state_.State() == RecvSourceState::kTerminal) {
    if (terminal_error_.has_value()) {
      result = std::unexpected(*terminal_error_);
    } else {
      result = NextResult(std::in_place, std::nullopt);
    }
    return true;
  }
  return false;
}

void RecvSource::QueueEvent(std::uint32_t buffer_id, std::size_t size) noexcept {
  ALYRN_CHECK(!events_.empty() && event_count_ < events_.size(),
                 "RecvSource event queue overflow");
  const std::size_t index = (event_head_ + event_count_) % events_.size();
  events_[index] = PendingEvent{.buffer_id = buffer_id, .size = size};
  ++event_count_;
}

bool RecvSource::TryTakeQueuedEvent(PendingEvent& event) noexcept {
  if (event_count_ == 0) {
    return false;
  }

  event = events_[event_head_];
  event_head_ = (event_head_ + 1) % events_.size();
  --event_count_;
  return true;
}

Result<void> RecvSource::AcquireBuffer(std::uint32_t buffer_id, std::size_t size) noexcept {
  if (buffer_pool_ == nullptr || buffer_id >= slots_.size() || size > buffer_size_) {
    return std::unexpected(Errno(EPROTO));
  }

  SlotState& slot = slots_[buffer_id];
  if (!slot.active) {
    if (!buffer_pool_->Acquire(buffer_id)) {
      return std::unexpected(Errno(EPROTO));
    }
    slot.active = true;
    slot.kernel_done = false;
    slot.lease_count = 0;
  } else {
    return std::unexpected(Errno(EPROTO));
  }

  ++slot.lease_count;
  return {};
}

void RecvSource::MarkKernelDone(std::uint32_t buffer_id) noexcept {
  ALYRN_CHECK(buffer_id < slots_.size() && slots_[buffer_id].active,
                 "RecvSource completed an unknown provided buffer");
  SlotState& slot = slots_[buffer_id];
  ALYRN_CHECK(!slot.kernel_done, "RecvSource completed a provided buffer twice");
  slot.kernel_done = true;
  ReturnIfReclaimable(buffer_id);
}

void RecvSource::MarkActiveSlotsKernelDone() noexcept {
  for (std::uint32_t buffer_id = 0; buffer_id < slots_.size(); ++buffer_id) {
    if (slots_[buffer_id].active && !slots_[buffer_id].kernel_done) {
      MarkKernelDone(buffer_id);
    }
  }
}

void RecvSource::ReturnIfReclaimable(std::uint32_t buffer_id) noexcept {
  SlotState& slot = slots_[buffer_id];
  if (!slot.active || !slot.kernel_done || slot.lease_count != 0) {
    return;
  }
  ALYRN_CHECK(buffer_pool_ != nullptr, "RecvSource missing provided buffer pool");
  ALYRN_CHECK(buffer_pool_->Return(buffer_id),
                 "RecvSource failed to return provided buffer");
  slot = SlotState{};
}

void RecvSource::ReleaseSlotLease(std::uint32_t buffer_id) noexcept {
  ALYRN_CHECK(
      buffer_id < slots_.size() && slots_[buffer_id].active && slots_[buffer_id].lease_count != 0,
      "RecvSource provided buffer lease released twice");
  --slots_[buffer_id].lease_count;
  ReturnIfReclaimable(buffer_id);
}

net::BufferLease RecvSource::MakeLease(std::uint32_t buffer_id, std::size_t size) noexcept {
  ALYRN_CHECK(buffer_pool_ != nullptr, "RecvSource missing provided buffer pool");
  ALYRN_CHECK(buffer_id < slots_.size() && slots_[buffer_id].active,
                 "RecvSource lease refers to an inactive provided buffer");
  auto* data = buffer_pool_->slot(buffer_id);
  ALYRN_CHECK(data != nullptr && size <= buffer_size_,
                 "RecvSource lease exceeds provided buffer");
  return net::BufferLease(data, size, buffer_id, this, &ReclaimBuffer);
}

void RecvSource::ReturnBuffer(std::uint32_t buffer_id) noexcept {
  ALYRN_CHECK(buffer_pool_ != nullptr, "RecvSource missing provided buffer pool");
  ReleaseSlotLease(buffer_id);
  ALYRN_CHECK(state_.ReleaseLease(), "RecvSource buffer lease released twice");

  // A live multishot request already has admission. In the common direct
  // delivery path, releasing the lease cannot re-arm anything or complete a
  // pending Stop, so avoid two cold-path probes for every received buffer.
  if (state_.State() == RecvSourceState::kActive && recv_submitted_ && pending_stop_ == nullptr) {
    return;
  }

  EnsureSubmission();
  CompleteStopIfReady();
}

void RecvSource::ReclaimBuffer(void* context, std::uint32_t buffer_id) noexcept {
  static_cast<RecvSource*>(context)->ReturnBuffer(buffer_id);
}

Result<void> RecvSource::RequestStop() noexcept {
  if (loop_ == nullptr || fd_ < 0) {
    return std::unexpected(Errno(EBADF));
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

coro::Task<Result<void>> RecvSource::Stop() {
  if (loop_ == nullptr) {
    co_return Result<void>{};
  }
  if (!loop_->IsInLoopThread()) {
    co_return std::unexpected(Errno(EINVAL));
  }
  if (pending_stop_ != nullptr) {
    co_return std::unexpected(Errno(EBUSY));
  }

  if (state_.State() == RecvSourceState::kTerminal && !recv_submitted_ && !cancel_submitted_) {
    co_return Result<void>{};
  }

  co_return co_await StopAwaiter(*this);
}

namespace detail {

CompletionDisposition DispatchRecvSourceComplete(::alyrn::uring::detail::Op* op, CompletionEvent event) noexcept {
  auto* operation = static_cast<RecvSource::RecvOperation*>(op);
  return operation->Source()->OnCompletion(event);
}

void DispatchRecvSourceCancelComplete(::alyrn::uring::detail::Op* op) noexcept {
  auto* operation = static_cast<RecvSource::CancelOperation*>(op);

  int result = -EIO;
  if (op->result.HasValue()) {
    result = *op->result;
  }
  operation->Source()->OnCancelComplete(result);
}

}  // namespace detail

}  // namespace alyrn::uring
