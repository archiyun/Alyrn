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

namespace coropact::luring {

namespace {

std::size_t BufferCapacity(const net::detail::RecvSourceStateMachine& state) noexcept {
  return state.Options().buffer_capacity;
}

base::Error SetupBufferRingError(int error) noexcept {
  if (error < 0) {
    return base::MakeNegErrno(error);
  }
  return base::MakeErrno(error == 0 ? EIO : error);
}

}  // namespace

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
      !loop->HasCapability(NativeFeature::kProvidedBufferRing) ||
      (options.incremental_buffer_consumption &&
       !loop->HasCapability(NativeFeature::kProvidedBufferRingIncremental))) {
    return std::unexpected(base::MakeErrno(ENOTSUP));
  }

  const std::size_t capacity = options.source.buffer_capacity;
  if (capacity > std::numeric_limits<std::size_t>::max() / options.buffer_size) {
    return std::unexpected(base::MakeErrno(EOVERFLOW));
  }

  auto state_result = net::detail::RecvSourceStateMachine::Create(options.source);
  if (!state_result.has_value()) {
    return std::unexpected(state_result.error());
  }

  auto group_result = loop->AllocateBufferGroupId();
  if (!group_result.has_value()) {
    return std::unexpected(group_result.error());
  }

  std::vector<std::byte> storage;
  std::vector<LUringRecvSource::BufferState> buffer_states;
  try {
    storage.resize(capacity * options.buffer_size);
    buffer_states.resize(capacity);
  } catch (...) {
    return std::unexpected(base::MakeErrno(ENOMEM));
  }

  int setup_error = 0;
  io_uring_buf_ring* buffer_ring = io_uring_setup_buf_ring(
      loop->ring_.Native(),
      static_cast<unsigned>(capacity),
      static_cast<int>(*group_result),
      options.incremental_buffer_consumption ? IOU_PBUF_RING_INC : 0,
      &setup_error);
  if (buffer_ring == nullptr) {
    return std::unexpected(SetupBufferRingError(setup_error));
  }

  const int mask = io_uring_buf_ring_mask(static_cast<unsigned>(capacity));
  io_uring_buf_ring_init(buffer_ring);
  for (std::size_t i = 0; i < capacity; ++i) {
    io_uring_buf_ring_add(
        buffer_ring,
        storage.data() + i * options.buffer_size,
        static_cast<unsigned>(options.buffer_size),
        static_cast<unsigned short>(i),
        mask,
        static_cast<int>(i));
  }
  io_uring_buf_ring_advance(buffer_ring, static_cast<int>(capacity));

  return LUringRecvSource(
      loop,
      fd,
      std::move(*state_result),
      buffer_ring,
      *group_result,
      options.buffer_size,
      options.incremental_buffer_consumption,
      std::move(buffer_states),
      std::move(storage));
}

LUringRecvSource::LUringRecvSource(
    LUringLoop* loop,
    int fd,
    net::detail::RecvSourceStateMachine state,
    io_uring_buf_ring* buffer_ring,
    std::uint16_t buffer_group,
    std::size_t buffer_size,
    bool incremental_buffer_consumption,
    std::vector<BufferState> buffer_states,
    std::vector<std::byte> storage) noexcept
    : loop_(loop),
      fd_(fd),
      state_(std::move(state)),
      recv_op_(this),
      cancel_op_(this),
      buffer_ring_(buffer_ring),
      buffer_group_(buffer_group),
      buffer_size_(buffer_size),
      incremental_buffer_consumption_(incremental_buffer_consumption),
      buffer_states_(std::move(buffer_states)),
      storage_(std::move(storage)) {}

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
  COROPACT_DCHECK(state_.State() == net::detail::RecvSourceState::kIdle ||
                     state_.State() == net::detail::RecvSourceState::kTerminal,
                 "LUringRecvSource destroyed before Stop completed");
  COROPACT_DCHECK(events_.empty(),
                 "LUringRecvSource destroyed with queued events");
  COROPACT_DCHECK(state_.OutstandingLeases() == 0,
                 "LUringRecvSource destroyed with outstanding leases");
  COROPACT_DCHECK(!active_incremental_buffer_.has_value(),
                 "LUringRecvSource destroyed with active incremental buffer");
  for (const auto& buffer : buffer_states_) {
    COROPACT_DCHECK(!buffer.in_use && buffer.leases == 0,
                   "LUringRecvSource destroyed with unreclaimed buffer");
  }

  ReleaseBufferRing();
}

LUringRecvSource::LUringRecvSource(LUringRecvSource&& other) noexcept
    : loop_(std::exchange(other.loop_, nullptr)),
      fd_(std::exchange(other.fd_, -1)),
      state_(std::move(other.state_)),
      events_(std::move(other.events_)),
      terminal_error_(std::move(other.terminal_error_)),
      recv_op_(this),
      cancel_op_(this),
      buffer_ring_(std::exchange(other.buffer_ring_, nullptr)),
      buffer_group_(std::exchange(other.buffer_group_, 0)),
      buffer_size_(std::exchange(other.buffer_size_, 0)),
      incremental_buffer_consumption_(std::exchange(
          other.incremental_buffer_consumption_, false)),
      active_incremental_buffer_(std::exchange(
          other.active_incremental_buffer_, std::nullopt)),
      buffer_states_(std::move(other.buffer_states_)),
      storage_(std::move(other.storage_)),
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
  COROPACT_CHECK(other.state_.State() == net::detail::RecvSourceState::kIdle ||
                     other.state_.State() == net::detail::RecvSourceState::kTerminal,
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
  COROPACT_CHECK(other.state_.State() == net::detail::RecvSourceState::kIdle ||
                     other.state_.State() == net::detail::RecvSourceState::kTerminal,
                 "LUringRecvSource source has not completed Stop");
  COROPACT_CHECK(other.events_.empty(),
                 "LUringRecvSource source has queued events");

  ReleaseBufferRing();
  loop_ = std::exchange(other.loop_, nullptr);
  fd_ = std::exchange(other.fd_, -1);
  state_ = std::move(other.state_);
  terminal_error_ = std::move(other.terminal_error_);
  buffer_ring_ = std::exchange(other.buffer_ring_, nullptr);
  buffer_group_ = std::exchange(other.buffer_group_, 0);
  buffer_size_ = std::exchange(other.buffer_size_, 0);
  incremental_buffer_consumption_ = std::exchange(
      other.incremental_buffer_consumption_, false);
  active_incremental_buffer_ = std::exchange(
      other.active_incremental_buffer_, std::nullopt);
  buffer_states_ = std::move(other.buffer_states_);
  storage_ = std::move(other.storage_);
  return *this;
}

base::Result<void> LUringRecvSource::StartOperation() noexcept {
  if (loop_ == nullptr || !loop_->Initialized() || fd_ < 0) {
    return std::unexpected(base::MakeErrno(EBADF));
  }
  if (recv_submitted_) {
    return {};
  }
  if (!state_.CanArm() || !state_.TryArm()) {
    return {};
  }

  recv_op_.Prepare();
  auto submitted = loop_->SubmitOp(
      &recv_op_,
      [fd = fd_, buffer_size = buffer_size_, buffer_group = buffer_group_](
          io_uring_sqe* sqe) noexcept {
        io_uring_prep_recv_multishot(sqe, fd, nullptr, buffer_size, 0);
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = buffer_group;
      });

  if (!submitted.has_value()) {
    auto completed = state_.CompleteMultishotEvent(
        net::detail::EventDisposition::kNone,
        net::detail::MultishotRequestDisposition::kTerminal);
    assert(completed.has_value());
    return std::unexpected(submitted.error());
  }

  recv_submitted_ = true;
  return {};
}

base::Result<void> LUringRecvSource::Start() noexcept {
  if (state_.State() != net::detail::RecvSourceState::kIdle) {
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

  return state_.State() != net::detail::RecvSourceState::kTerminal ||
         recv_submitted_ || cancel_submitted_;
}

void LUringRecvSource::EnsureSubmission() noexcept {
  if (loop_ == nullptr || !loop_->Initialized() ||
      state_.State() != net::detail::RecvSourceState::kActive ||
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

void LUringRecvSource::FinalizeActiveIncrementalBuffer() noexcept {
  if (!active_incremental_buffer_.has_value()) {
    return;
  }

  const std::uint32_t buffer_id = *active_incremental_buffer_;
  active_incremental_buffer_.reset();
  COROPACT_CHECK(buffer_id < buffer_states_.size(),
                 "LUringRecvSource invalid active incremental buffer");
  buffer_states_[buffer_id].final_seen = true;
  MaybeReturnBuffer(buffer_id);
}

void LUringRecvSource::MaybeReturnBuffer(std::uint32_t buffer_id) noexcept {
  COROPACT_CHECK(buffer_id < buffer_states_.size(),
                 "LUringRecvSource invalid buffer state");
  auto& buffer = buffer_states_[buffer_id];
  if (!buffer.in_use || !buffer.final_seen || buffer.leases != 0) {
    return;
  }

  ReturnBufferToRing(buffer_id);
  buffer = {};
}

void LUringRecvSource::HoldOrFinalizeBuffer(
    std::uint32_t buffer_id,
    bool more_completions) noexcept {
  COROPACT_CHECK(buffer_id < buffer_states_.size(),
                 "LUringRecvSource invalid buffer state");
  auto& buffer = buffer_states_[buffer_id];
  buffer.in_use = true;
  if (more_completions) {
    buffer.final_seen = false;
    active_incremental_buffer_ = buffer_id;
    return;
  }

  buffer.final_seen = true;
  if (active_incremental_buffer_.has_value() &&
      *active_incremental_buffer_ == buffer_id) {
    active_incremental_buffer_.reset();
  }
  MaybeReturnBuffer(buffer_id);
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
  const bool valid_buffer =
      has_buffer && buffer_id < BufferCapacity(state_);

  bool state_recorded = false;
  bool buffer_prepared = false;
  std::size_t buffer_offset = 0;
  std::optional<base::Error> completion_error;

  if (event.BufferMore() && !incremental_buffer_consumption_) {
    completion_error = base::MakeErrno(EPROTO);
  }

  if (cqe_result > 0 && completion_error.has_value() == false) {
    if (!valid_buffer) {
      completion_error = base::MakeErrno(EPROTO);
    } else {
      auto& buffer = buffer_states_[buffer_id];
      const bool new_buffer = !buffer.in_use;
      if (new_buffer) {
        if (incremental_buffer_consumption_ &&
            active_incremental_buffer_.has_value()) {
          completion_error = base::MakeErrno(EPROTO);
        } else {
          buffer = {};
          buffer.in_use = true;
        }
      } else if (!incremental_buffer_consumption_ ||
                 !active_incremental_buffer_.has_value() ||
                 *active_incremental_buffer_ != buffer_id ||
                 buffer.final_seen) {
        completion_error = base::MakeErrno(EPROTO);
      }

      if (!completion_error.has_value()) {
        buffer_offset = buffer.offset;
        if (buffer_offset > buffer_size_ ||
            static_cast<std::size_t>(cqe_result) >
            buffer_size_ - buffer_offset) {
          completion_error = base::MakeErrno(EOVERFLOW);
        } else {
          buffer.offset += static_cast<std::size_t>(cqe_result);
          buffer.final_seen = !event.BufferMore();
          if (event.BufferMore()) {
            active_incremental_buffer_ = buffer_id;
          } else if (active_incremental_buffer_.has_value() &&
                     *active_incremental_buffer_ == buffer_id) {
            active_incremental_buffer_.reset();
          }
          buffer_prepared = true;
        }
      }
    }
  }

  if (cqe_result > 0) {
    if (completion_error.has_value()) {
      if (valid_buffer) {
        HoldOrFinalizeBuffer(buffer_id, event.BufferMore() && request_still_active);
      } else {
        FinalizeActiveIncrementalBuffer();
      }
      RequestBackendStop(*completion_error);
    } else if (!state_.CanQueueEvent()) {
      if (buffer_prepared) {
        MaybeReturnBuffer(buffer_id);
      }
      RequestBackendPause();
    } else {
      auto recorded = state_.CompleteMultishotEvent(
          net::detail::EventDisposition::kProduced,
          request_still_active ? net::detail::MultishotRequestDisposition::kMore
                               : net::detail::MultishotRequestDisposition::kTerminal);
      if (!recorded.has_value()) {
        // The failed accounting attempt did not consume the state-machine
        // event; the fallback below will record its terminal/non-terminal
        // request completion.
        buffer_states_[buffer_id].final_seen = true;
        MaybeReturnBuffer(buffer_id);
        RequestBackendStop(recorded.error());
      } else {
        state_recorded = true;
        ++buffer_states_[buffer_id].leases;
        net::BufferLease lease(
            storage_.data() + static_cast<std::size_t>(buffer_id) * buffer_size_ +
                buffer_offset,
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
    if (valid_buffer) {
      HoldOrFinalizeBuffer(buffer_id, event.BufferMore() && request_still_active);
    } else if (!request_still_active) {
      FinalizeActiveIncrementalBuffer();
    }
    if (!request_still_active) {
      RequestBackendStop();
    }
  } else {
    if (valid_buffer) {
      HoldOrFinalizeBuffer(buffer_id, event.BufferMore() && request_still_active);
    } else if (!request_still_active) {
      FinalizeActiveIncrementalBuffer();
    }
    const auto state = state_.State();
    const bool stopping = state == net::detail::RecvSourceState::kStopping ||
                          state == net::detail::RecvSourceState::kPausing ||
                          state == net::detail::RecvSourceState::kPaused ||
                          state == net::detail::RecvSourceState::kDraining ||
                          state == net::detail::RecvSourceState::kTerminal;
    if (!stopping) {
      RequestBackendStop(base::MakeNegErrno(cqe_result));
    }
  }

  if (!request_still_active) {
    FinalizeActiveIncrementalBuffer();
  }

  if (!state_recorded) {
    auto recorded = state_.CompleteMultishotEvent(
        net::detail::EventDisposition::kNone,
        request_still_active ? net::detail::MultishotRequestDisposition::kMore
                             : net::detail::MultishotRequestDisposition::kTerminal);
    if (!recorded.has_value()) {
      terminal_error_ = recorded.error();
      RequestBackendStop(recorded.error());
    }
  }

  if (!request_still_active) {
    recv_op_.BeginNextRequest();
  }

  if (state_.State() == net::detail::RecvSourceState::kActive &&
      state_.QueuedEvents() >= state_.Options().event_capacity) {
    RequestBackendPause();
  }

  if (!request_still_active && state_.State() == net::detail::RecvSourceState::kActive &&
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
      state_.State() != net::detail::RecvSourceState::kTerminal) {
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
    if (state_.State() == net::detail::RecvSourceState::kPaused) {
      MaybeResume();
    }
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

void LUringRecvSource::ReturnBufferToRing(std::uint32_t buffer_id) noexcept {
  COROPACT_CHECK(loop_ != nullptr && loop_->IsInLoopThread(),
                 "LUringRecvSource buffer returned from wrong thread");
  COROPACT_CHECK(buffer_ring_ != nullptr && buffer_id < BufferCapacity(state_),
                 "LUringRecvSource invalid provided buffer id");

  const int mask = io_uring_buf_ring_mask(
      static_cast<unsigned>(BufferCapacity(state_)));
  io_uring_buf_ring_add(
      buffer_ring_,
      storage_.data() + static_cast<std::size_t>(buffer_id) * buffer_size_,
      static_cast<unsigned>(buffer_size_),
      static_cast<unsigned short>(buffer_id),
      mask,
      0);
  io_uring_buf_ring_advance(buffer_ring_, 1);
}

void LUringRecvSource::ReturnBuffer(std::uint32_t buffer_id) noexcept {
  COROPACT_CHECK(buffer_id < buffer_states_.size(),
                 "LUringRecvSource invalid buffer state on release");
  auto& buffer = buffer_states_[buffer_id];
  COROPACT_CHECK(buffer.in_use && buffer.leases > 0,
                 "LUringRecvSource buffer lease released twice");
  --buffer.leases;
  COROPACT_CHECK(state_.ReleaseLease(),
                 "LUringRecvSource buffer lease released twice");
  MaybeReturnBuffer(buffer_id);
  EnsureSubmission();
  CompleteStopIfReady();
}

void LUringRecvSource::ReleaseBufferRing() noexcept {
  if (buffer_ring_ == nullptr) {
    return;
  }

  const int result = io_uring_free_buf_ring(
      loop_->ring_.Native(),
      buffer_ring_,
      static_cast<unsigned>(BufferCapacity(state_)),
      static_cast<int>(buffer_group_));
  COROPACT_DCHECK(result == 0,
                 "LUringRecvSource failed to unregister provided buffer ring");
  buffer_ring_ = nullptr;
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

  if (state_.State() == net::detail::RecvSourceState::kIdle) {
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

  if (state_.State() == net::detail::RecvSourceState::kTerminal &&
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
