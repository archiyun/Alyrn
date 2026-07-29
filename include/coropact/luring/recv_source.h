// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <liburing/io_uring.h>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <vector>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/backend/recv_source.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/op.h"
#include "coropact/luring/options.h"
#include "coropact/net/recv_source.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

class LUringLoop;

struct LUringRecvSourceOptions {
  net::RecvSourceOptions source{};
  std::size_t buffer_size{16 * 1024};
  bool incremental_buffer_consumption{false};

  [[nodiscard]]
  bool Valid() const noexcept {
    const std::size_t capacity = source.buffer_capacity;
    return source.Valid() && source.pending_depth == 1 && buffer_size > 0 &&
           buffer_size <= std::numeric_limits<std::uint32_t>::max() &&
           capacity <= 32 * 1024 && (capacity & (capacity - 1)) == 0;
  }
};

// A loop-affine multishot recv source backed by an io_uring provided-buffer
// ring. The socket fd is borrowed and must outlive this source. The source
// must remain alive until every BufferLease returned by Next() has been
// released; Stop() waits for that lease boundary before completing.
class LUringRecvSource final {
  friend void detail::DispatchRecvSourceComplete(
      LUringOp* op,
      CompletionEvent event) noexcept;
  friend void detail::DispatchRecvSourceCancelComplete(LUringOp* op) noexcept;

public:
  COROPACT_DELETE_COPY(LUringRecvSource);

  using Event = net::RecvEvent;
  using Result = base::Result<std::optional<Event>>;

  [[nodiscard]]
  static base::Result<LUringRecvSource> Create(
      LUringLoop* loop,
      int fd,
      LUringRecvSourceOptions options = {}) noexcept;

  ~LUringRecvSource();

  LUringRecvSource(LUringRecvSource&& other) noexcept;
  LUringRecvSource& operator=(LUringRecvSource&& other) noexcept;

  coro::Task<Result> Next();

  // Begins cancellation without waiting for queued events or outstanding
  // BufferLease instances. This lets an owning adapter stop admission, drain
  // its already-produced events, and then await Stop() for the final lease
  // boundary.
  [[nodiscard]]
  base::Result<void> RequestStop() noexcept;

  coro::Task<base::Result<void>> Stop();

private:
  struct BufferState {
    std::size_t offset{0};
    std::size_t leases{0};
    bool in_use{false};
    bool final_seen{false};
  };

  class NextAwaiter;
  class StopAwaiter;

  class RecvOperation final : public LUringOp {
  public:
    explicit RecvOperation(LUringRecvSource* source) noexcept
        : source_(source) {
      kind = LUringOpKind::kRecvSourceComplete;
    }

    [[nodiscard]]
    LUringRecvSource* Source() const noexcept {
      return source_;
    }

    void Prepare() noexcept {
      kind = LUringOpKind::kRecvSourceComplete;
      ResetCompletion();
      result = {};
      resume_work.ClearHandle();
    }

  private:
    LUringRecvSource* source_;
  };

  class CancelOperation final : public LUringOp {
  public:
    explicit CancelOperation(LUringRecvSource* source) noexcept
        : source_(source) {
      kind = LUringOpKind::kRecvSourceCancelComplete;
    }

    [[nodiscard]]
    LUringRecvSource* Source() const noexcept {
      return source_;
    }

    void Prepare() noexcept {
      kind = LUringOpKind::kRecvSourceCancelComplete;
      ResetCompletion();
      result = {};
      resume_work.ClearHandle();
    }

  private:
    LUringRecvSource* source_;
  };

  LUringRecvSource(
      LUringLoop* loop,
      int fd,
      net::detail::RecvSourceStateMachine state,
      io_uring_buf_ring* buffer_ring,
      std::uint16_t buffer_group,
      std::size_t buffer_size,
      bool incremental_buffer_consumption,
      std::vector<BufferState> buffer_states,
      std::vector<std::byte> storage) noexcept;

  [[nodiscard]]
  base::Result<void> Start() noexcept;

  [[nodiscard]]
  base::Result<void> StartOperation() noexcept;

  [[nodiscard]]
  base::Result<void> StartCancel() noexcept;

  [[nodiscard]]
  base::Result<bool> BeginStop() noexcept;

  void EnsureSubmission() noexcept;
  void RequestBackendStop(
      std::optional<base::Error> error = std::nullopt) noexcept;

  void OnCompletion(CompletionEvent event) noexcept;
  void OnCancelComplete(int cqe_result) noexcept;

  void DeliverNextIfReady() noexcept;
  void CompleteStopIfReady() noexcept;
  bool TryTakeNext(Result& result) noexcept;
  void ReturnBuffer(std::uint32_t buffer_id) noexcept;
  void ReturnBufferToRing(std::uint32_t buffer_id) noexcept;
  void HoldOrFinalizeBuffer(
      std::uint32_t buffer_id,
      bool more_completions) noexcept;
  void FinalizeActiveIncrementalBuffer() noexcept;
  void MaybeReturnBuffer(std::uint32_t buffer_id) noexcept;
  void ReleaseBufferRing() noexcept;

  static void ReclaimBuffer(void* context, std::uint32_t buffer_id) noexcept;

  LUringLoop* loop_{nullptr};
  int fd_{-1};
  net::detail::RecvSourceStateMachine state_;
  std::deque<Event> events_;
  std::optional<base::Error> terminal_error_;

  NextAwaiter* pending_next_{nullptr};
  StopAwaiter* pending_stop_{nullptr};

  RecvOperation recv_op_;
  CancelOperation cancel_op_;

  io_uring_buf_ring* buffer_ring_{nullptr};
  std::uint16_t buffer_group_{0};
  std::size_t buffer_size_{0};
  bool incremental_buffer_consumption_{false};
  std::optional<std::uint32_t> active_incremental_buffer_;
  std::vector<BufferState> buffer_states_;
  std::vector<std::byte> storage_;

  bool recv_submitted_{false};
  bool cancel_submitted_{false};
};

static_assert(backend::AsyncRecvSource<LUringRecvSource>);

}  // namespace coropact::luring
