// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <sys/socket.h>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>

#include "coropact/backend/accept_source.h"
#include "coropact/backend/async_listener.h"
#include "coropact/backend/detail/value_result_state.h"
#include "coropact/result.h"
#include "coropact/coro/task.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/stream.h"
#include "coropact/net/accept_source.h"
#include "coropact/net/endpoint.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

class LUringLoop;
class LUringListener;

struct LUringListenOptions {
  bool reuse_addr{true};
  bool reuse_port{true};
  // Exposes the luring stream's explicit zero-copy send extension to generic
  // WriteAll(). It is opt-in; the physical single-send operation remains
  // internal to LUringStream.
  bool zero_copy_writes{false};
  int backlog{SOMAXCONN};
  // Number of accepts kept in flight by each worker. A value greater than one
  // prevents a connection burst from being serialized behind one accept CQE.
  std::size_t accept_depth{4};
};

class LUringAcceptSource {
  friend class LUringListener;

  friend detail::CompletionDisposition detail::DispatchAcceptSourceComplete(
      detail::LUringOp* op, detail::CompletionEvent event) noexcept;

  friend void detail::DispatchAcceptSourceCancelComplete(detail::LUringOp* op) noexcept;

public:
  COROPACT_DELETE_COPY(LUringAcceptSource);

  using Stream = LUringStream;
  using Event = std::optional<Stream>;
  using NextResult = coropact::Result<Event>;

  // Direct awaiter for the single-consumer accept loop. It preserves the
  // source's multishot/one-shot physical implementation while avoiding a
  // child Task frame for each logical accept event.
  class NextAwaiter {
  public:
    explicit NextAwaiter(LUringAcceptSource& source) noexcept : source_(&source) {}

    [[nodiscard]]
    bool await_ready() const noexcept {
      return false;
    }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept;

    NextResult await_resume() noexcept;

    void Complete(NextResult result) noexcept;

  private:
    LUringAcceptSource* source_;
    operation::detail::SchedulerContinuation continuation_;
    operation::detail::CompletionGate completion_gate_;
    backend::detail::ValueResultState<Event> result_;
  };

  ~LUringAcceptSource();

  LUringAcceptSource(LUringAcceptSource&& other) noexcept;
  LUringAcceptSource& operator=(LUringAcceptSource&& other) noexcept;

  [[nodiscard]]
  NextAwaiter Next() noexcept {
    return NextAwaiter(*this);
  }
  coro::Task<Result<void>> Stop();

private:
  class StopAwaiter;

  class AcceptOperation final : public detail::LUringOp {
  public:
    explicit AcceptOperation(LUringAcceptSource* source) noexcept : source_(source) {
      kind = detail::LUringOpKind::kAcceptSourceComplete;
    }

    [[nodiscard]]
    LUringAcceptSource* Source() const noexcept {
      return source_;
    }

    void Prepare() noexcept {
      kind = detail::LUringOpKind::kAcceptSourceComplete;
      BeginNextRequest();
    }

  private:
    LUringAcceptSource* source_;
  };

  class CancelOperation final : public detail::LUringOp {
  public:
    explicit CancelOperation(LUringAcceptSource* source) noexcept : source_(source) {
      kind = detail::LUringOpKind::kAcceptSourceCancelComplete;
    }

    [[nodiscard]]
    LUringAcceptSource* Source() const noexcept {
      return source_;
    }

    void Prepare() noexcept {
      kind = detail::LUringOpKind::kAcceptSourceCancelComplete;
      BeginNextRequest();
    }

  private:
    LUringAcceptSource* source_;
  };

  LUringAcceptSource(LUringListener* listener,
                     net::detail::AcceptSourceStateMachine state) noexcept;

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
  void OnCancelComplete(int cqe_res) noexcept;
  void OnListenerClosed() noexcept;

  void DeliverNextIfReady() noexcept;
  void CompleteStopIfReady() noexcept;
  bool TryTakeNext(NextResult& result) noexcept;
  void ReleaseListenerReservation() noexcept;

  [[nodiscard]]
  Result<LUringStream> MakeStream(int accepted_fd) noexcept;

  LUringListener* listener_{nullptr};
  net::detail::AcceptSourceStateMachine state_;
  std::deque<LUringStream> events_;
  std::optional<Error> terminal_error_;

  NextAwaiter* pending_next_{nullptr};
  StopAwaiter* pending_stop_{nullptr};

  AcceptOperation accept_op_;
  CancelOperation cancel_op_;

  bool accept_submitted_{false};
  bool cancel_submitted_{false};

  // Multishot accept is attempted first.  A terminal unsupported CQE switches
  // this source to the single-shot accept path; subsequent accepts keep the
  // same logical source contract without requiring a backend restart.
  bool multishot_enabled_{true};
};

static_assert(backend::AsyncAcceptSource<LUringAcceptSource>);

class LUringListener {
  friend class LUringAcceptSource;
  friend void detail::DispatchAcceptComplete(detail::LUringOp* op) noexcept;
  friend void detail::DispatchListenerCloseComplete(detail::LUringOp* op) noexcept;

public:
  COROPACT_DELETE_COPY(LUringListener);

  using Stream = LUringStream;

  static Result<LUringListener> Create(LUringLoop* loop, const net::Endpoint& listen_addr,
                                             LUringListenOptions options = {}) noexcept;

  ~LUringListener();

  LUringListener(LUringListener&& other) noexcept;
  LUringListener& operator=(LUringListener&& other) noexcept;

  // Accept, Close, and AcceptSource are loop-affine. Their coroutine or
  // factory call must execute on this listener's owner LUringLoop; a foreign
  // thread is a runtime-contract violation checked in every build.
  coro::Task<Result<LUringStream>> Accept();

  [[nodiscard]]
  Result<LUringAcceptSource> AcceptSource(net::AcceptSourceOptions options = {}) noexcept;

  coro::Task<Result<void>> Close();

  [[nodiscard]]
  Result<net::Endpoint> LocalAddress() const noexcept;

  [[nodiscard]]
  int Fd() const noexcept {
    return fd_;
  }

private:
  class AcceptAwaiter;
  class CloseAwaiter;

  [[nodiscard]]
  LUringListener(LUringLoop* loop, int fd, bool zero_copy_writes) noexcept;

  void RequireOwnerLoop() const noexcept;
  void NotifyCloseProgress() noexcept;
  void ResetForMove() noexcept;
  static LUringLoop* PrepareMove(LUringListener& other) noexcept;

  LUringLoop* loop_;
  int fd_{-1};
  std::size_t pending_accepts_{0};
  CloseAwaiter* pending_close_{nullptr};
  LUringAcceptSource* accept_source_{nullptr};
  bool zero_copy_writes_{false};
  bool closed_{false};
};

static_assert(backend::AsyncListener<LUringListener>);

}  // namespace coropact::luring
