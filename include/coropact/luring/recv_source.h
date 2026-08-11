// SPDX-License-Identifier: MIT
#pragma once

#include <liburing/io_uring.h>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <vector>

#include "coropact/backend/detail/value_result_state.h"
#include "coropact/backend/recv_source.h"
#include "coropact/result.h"
#include "coropact/coro/task.h"
#include "coropact/coro/work.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/options.h"
#include "coropact/net/recv_source.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

class LUringLoop;
namespace detail {
class ProvidedBufferPool;
}

struct LUringRecvSourceOptions {
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
class LUringRecvSource final {
  friend detail::CompletionDisposition detail::DispatchRecvSourceComplete(
      detail::LUringOp* op, detail::CompletionEvent event) noexcept;
  friend void detail::DispatchRecvSourceCancelComplete(detail::LUringOp* op) noexcept;

public:
  COROPACT_DELETE_COPY(LUringRecvSource);

  using Event = net::RecvEvent;
  using NextResult = coropact::Result<std::optional<Event>>;

  // Direct awaiter for the single-consumer receive loop. It keeps the same
  // result and ownership semantics without creating a child Task frame for
  // every received buffer.
  class NextAwaiter {
  public:
    explicit NextAwaiter(LUringRecvSource& source) noexcept : source_(&source) {}

    [[nodiscard]]
    bool await_ready() const noexcept {
      return false;
    }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept;

    NextResult await_resume() noexcept;

    void Complete(NextResult result) noexcept;

  private:
    LUringRecvSource* source_;
    coro::ResumeWork resume_work_;
    operation::detail::CompletionGate completion_gate_;
    backend::detail::ValueResultState<std::optional<Event>> result_;
  };

  [[nodiscard]]
  static Result<LUringRecvSource> Create(LUringLoop* loop, int fd,
                                               LUringRecvSourceOptions options = {}) noexcept;

  ~LUringRecvSource();

  LUringRecvSource(LUringRecvSource&& other) noexcept;
  LUringRecvSource& operator=(LUringRecvSource&& other) noexcept;

  [[nodiscard]]
  NextAwaiter Next() noexcept {
    return NextAwaiter(*this);
  }

  // Begins cancellation without waiting for queued events or outstanding
  // BufferLease instances. This lets an owning adapter stop admission, drain
  // its already-produced events, and then await Stop() for the final lease
  // boundary.
  [[nodiscard]]
  Result<void> RequestStop() noexcept;

  coro::Task<Result<void>> Stop();

private:
  class StopAwaiter;

  class RecvOperation final : public detail::LUringOp {
  public:
    explicit RecvOperation(LUringRecvSource* source) noexcept : source_(source) {
      kind = detail::LUringOpKind::kRecvSourceComplete;
    }

    [[nodiscard]]
    LUringRecvSource* Source() const noexcept {
      return source_;
    }

    void Prepare() noexcept {
      kind = detail::LUringOpKind::kRecvSourceComplete;
      BeginNextRequest();
    }

  private:
    LUringRecvSource* source_;
  };

  class CancelOperation final : public detail::LUringOp {
  public:
    explicit CancelOperation(LUringRecvSource* source) noexcept : source_(source) {
      kind = detail::LUringOpKind::kRecvSourceCancelComplete;
    }

    [[nodiscard]]
    LUringRecvSource* Source() const noexcept {
      return source_;
    }

    void Prepare() noexcept {
      kind = detail::LUringOpKind::kRecvSourceCancelComplete;
      BeginNextRequest();
    }

  private:
    LUringRecvSource* source_;
  };

  struct PendingEvent {
    std::uint32_t buffer_id{0};
    std::size_t size{0};
  };

  LUringRecvSource(LUringLoop* loop, int fd, net::detail::RecvSourceStateMachine state,
                   std::size_t buffer_size, detail::ProvidedBufferPool* shared_buffer_pool,
                   std::vector<PendingEvent> event_storage) noexcept;

  [[nodiscard]]
  Result<void> Start() noexcept;

  [[nodiscard]]
  Result<void> StartOperation() noexcept;

  [[nodiscard]]
  Result<void> StartCancel() noexcept;

  [[nodiscard]]
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
  void ReturnBuffer(std::uint32_t buffer_id) noexcept;

  static void ReclaimBuffer(void* context, std::uint32_t buffer_id) noexcept;

  LUringLoop* loop_{nullptr};
  int fd_{-1};
  net::detail::RecvSourceStateMachine state_;
  std::vector<PendingEvent> events_;
  std::size_t event_head_{0};
  std::size_t event_count_{0};
  std::optional<Error> terminal_error_;

  NextAwaiter* pending_next_{nullptr};
  StopAwaiter* pending_stop_{nullptr};

  RecvOperation recv_op_;
  CancelOperation cancel_op_;

  std::size_t buffer_size_{0};
  detail::ProvidedBufferPool* shared_buffer_pool_{nullptr};

  bool recv_submitted_{false};
  bool cancel_submitted_{false};
};

static_assert(backend::AsyncRecvSource<LUringRecvSource>);

}  // namespace coropact::luring
