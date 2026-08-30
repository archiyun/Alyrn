// SPDX-License-Identifier: MIT
#pragma once

#include <liburing/io_uring.h>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <vector>

#include "alyrn/backend/recv_source.h"
#include "alyrn/backend/value_result_state.h"
#include "alyrn/coro/work.h"
#include "alyrn/detail/completion_gate.h"
#include "alyrn/detail/macros.h"
#include "alyrn/net/recv_source.h"
#include "alyrn/result.h"
#include "alyrn/task.h"
#include "alyrn/uring/detail/completion_dispatch.h"
#include "alyrn/uring/detail/op.h"

namespace alyrn::uring {

class Loop;
namespace detail {
class ProvidedBufferPool;
}

struct RecvSourceOptions {
  net::RecvSourceOptions source{};
  std::size_t buffer_size{16 * 1024};

  [[nodiscard]]
  bool Valid() const noexcept {
    const std::size_t capacity = source.buffer_capacity;
    return source.Valid() && source.pending_depth == 1 && buffer_size > 0 &&
           buffer_size <= std::numeric_limits<std::uint32_t>::max() && capacity <= 32 * 1024 &&
           (capacity & (capacity - 1)) == 0;
  }
};

// A loop-affine multishot recv source backed by an io_uring provided-buffer
// ring. The socket fd is borrowed and must outlive this source. The source
// must remain alive until every BufferLease returned by Next() has been
// released; Stop() waits for that lease boundary before completing.
class RecvSource final {
  friend detail::CompletionDisposition detail::DispatchRecvSourceComplete(
      detail::Op* op, detail::CompletionEvent event) noexcept;
  friend void detail::DispatchRecvSourceCancelComplete(detail::Op* op) noexcept;

public:
  ALYRN_DELETE_COPY(RecvSource);

  using Event = net::RecvEvent;
  using NextResult = Result<std::optional<Event>>;

  // Direct awaiter for the single-consumer receive loop. It keeps the same
  // result and ownership semantics without creating a child Task frame for
  // every received buffer.
  class [[nodiscard]] NextAwaiter {
  public:
    explicit NextAwaiter(RecvSource& source) noexcept : source_(&source) {}

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> continuation) noexcept;
    NextResult await_resume() noexcept;

    void Complete(NextResult result) noexcept;

  private:
    RecvSource* source_;
    coro::ResumeWork resume_work_;
    ::alyrn::detail::CompletionGate completion_gate_;
    backend::ValueResultState<std::optional<Event>> result_;
  };

  [[nodiscard]]
  static Result<RecvSource> Create(Loop* loop, int fd, RecvSourceOptions options = {}) noexcept;

  ~RecvSource();

  RecvSource(RecvSource&& other) noexcept;
  RecvSource& operator=(RecvSource&& other) noexcept;

  NextAwaiter Next() noexcept { return NextAwaiter(*this); }

  // Begins cancellation without waiting for queued events or outstanding
  // BufferLease instances. This lets an owning adapter stop admission, drain
  // its already-produced events, and then await Stop() for the final lease
  // boundary.
  [[nodiscard]]
  Result<void> RequestStop() noexcept;

  Task<Result<void>> Stop();

private:
  class StopAwaiter;

  class RecvOperation final : public detail::Op {
  public:
    explicit RecvOperation(RecvSource* source) noexcept : source_(source) {
      kind = detail::OpKind::kRecvSourceComplete;
    }

    RecvSource* Source() const noexcept { return source_; }

    void Prepare() noexcept {
      kind = detail::OpKind::kRecvSourceComplete;
      BeginNextRequest();
    }

  private:
    RecvSource* source_;
  };

  class CancelOperation final : public detail::Op {
  public:
    explicit CancelOperation(RecvSource* source) noexcept : source_(source) {
      kind = detail::OpKind::kRecvSourceCancelComplete;
    }

    RecvSource* Source() const noexcept { return source_; }

    void Prepare() noexcept {
      kind = detail::OpKind::kRecvSourceCancelComplete;
      BeginNextRequest();
    }

  private:
    RecvSource* source_;
  };

  struct PendingEvent {
    std::uint32_t copy_slot{0};
    std::size_t size{0};
  };

  struct SlotState {
    std::size_t lease_count{0};
    bool active{false};
    bool kernel_done{false};
  };

  RecvSource(Loop* loop, int fd, net::detail::RecvSourceStateMachine state, std::size_t buffer_size,
             detail::ProvidedBufferPool* buffer_pool, std::vector<PendingEvent> event_storage,
             std::vector<SlotState> slot_storage, std::vector<std::byte> queued_payloads,
             std::vector<std::uint32_t> copy_free) noexcept;

  Result<void> Start() noexcept;

  Result<void> StartOperation() noexcept;

  Result<void> StartCancel() noexcept;

  Result<bool> BeginStop() noexcept;

  void EnsureSubmission() noexcept;
  void MaybeResume() noexcept;
  void RequestBackendPause() noexcept;
  void RequestBackendStop(std::optional<Error> error = std::nullopt) noexcept;

  detail::CompletionDisposition OnCompletion(detail::CompletionEvent event) noexcept;
  void OnCancelComplete(int cqe_result) noexcept;

  void DeliverNextIfReady() noexcept;
  void CompleteStopIfReady() noexcept;
  bool TryTakeNext(NextResult& result) noexcept;
  void QueueEvent(std::uint32_t buffer_id, std::size_t size) noexcept;
  bool TryTakeQueuedEvent(PendingEvent& event) noexcept;
  Result<void> AcquireBuffer(std::uint32_t buffer_id, std::size_t size) noexcept;
  void MarkKernelDone(std::uint32_t buffer_id) noexcept;
  void MarkActiveSlotsKernelDone() noexcept;
  void ReturnIfReclaimable(std::uint32_t buffer_id) noexcept;
  void ReleaseSlotLease(std::uint32_t buffer_id) noexcept;
  net::BufferLease MakeQueuedLease(std::uint32_t copy_slot, std::size_t size) noexcept;
  std::uint32_t CopyAndReleasePoolSlot(std::uint32_t buffer_id, std::size_t size) noexcept;
  void ReturnQueuedPayload(std::uint32_t copy_slot) noexcept;
  std::uint32_t TakeCopySlot() noexcept;
  void FreeCopySlot(std::uint32_t copy_slot) noexcept;

  static void ValidateMovable(const RecvSource& source) noexcept;
  static void ReclaimQueuedPayload(void* context, std::uint32_t copy_slot) noexcept;

  Loop* loop_{nullptr};
  int fd_{-1};
  net::detail::RecvSourceStateMachine state_;
  std::vector<PendingEvent> events_;
  std::vector<SlotState> slots_;
  std::vector<std::byte> queued_payloads_;
  std::vector<std::uint32_t> copy_free_;
  std::size_t event_head_{0};
  std::size_t event_count_{0};
  std::size_t active_slot_count_{0};
  std::optional<Error> terminal_error_;

  NextAwaiter* pending_next_{nullptr};
  StopAwaiter* pending_stop_{nullptr};

  RecvOperation recv_op_;
  CancelOperation cancel_op_;

  std::size_t buffer_size_{0};
  // Borrowed loop-shared provided-buffer pool. The loop outlives its sources.
  detail::ProvidedBufferPool* buffer_pool_{nullptr};

  bool recv_submitted_{false};
  bool cancel_submitted_{false};
};

static_assert(backend::AsyncRecvSource<RecvSource>);

}  // namespace alyrn::uring
