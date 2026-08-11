// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>

#include "coropact/backend/accept_source.h"
#include "coropact/backend/async_listener.h"
#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/detail/op.h"
#include "coropact/luring/stream.h"
#include "coropact/net/accept_source.h"
#include "coropact/net/endpoint.h"
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
      detail::LUringOp* op,
      detail::CompletionEvent event) noexcept;

  friend void detail::DispatchAcceptSourceCancelComplete(detail::LUringOp* op) noexcept;

public:
  COROPACT_DELETE_COPY(LUringAcceptSource);

  using Stream = LUringStream;
  using Event = std::optional<Stream>;
  using Result = base::Result<Event>;

  ~LUringAcceptSource();

  LUringAcceptSource(LUringAcceptSource&& other) noexcept;
  LUringAcceptSource& operator=(LUringAcceptSource&& other) noexcept;

  coro::Task<Result> Next();
  coro::Task<base::Result<void>> Stop();

private:
  class NextAwaiter;
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
  base::Result<void> Start() noexcept;

  [[nodiscard]]
  base::Result<void> StartOperation() noexcept;

  [[nodiscard]]
  base::Result<void> StartCancel() noexcept;

  [[nodiscard]]
  base::Result<bool> BeginStop() noexcept;

  void EnsureSubmission() noexcept;
  void MaybeResume() noexcept;
  void RequestBackendPause() noexcept;
  void RequestBackendStop(std::optional<base::Error> error = std::nullopt) noexcept;

  detail::CompletionDisposition OnCompletion(detail::CompletionEvent event) noexcept;
  void OnCancelComplete(int cqe_res) noexcept;
  void OnListenerClosed() noexcept;

  void DeliverNextIfReady() noexcept;
  void CompleteStopIfReady() noexcept;
  bool TryTakeNext(Result& result) noexcept;
  void ReleaseListenerReservation() noexcept;

  [[nodiscard]]
  base::Result<LUringStream> MakeStream(int accepted_fd) noexcept;

  LUringListener* listener_{nullptr};
  net::detail::AcceptSourceStateMachine state_;
  std::deque<LUringStream> events_;
  std::optional<base::Error> terminal_error_;

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

  static base::Result<LUringListener> Create(LUringLoop* loop, const net::Endpoint& listen_addr,
                                             LUringListenOptions options = {}) noexcept;

  ~LUringListener();

  LUringListener(LUringListener&& other) noexcept;
  LUringListener& operator=(LUringListener&& other) noexcept;

  // Accept, Close, and AcceptSource are loop-affine. Their coroutine or
  // factory call must execute on this listener's owner LUringLoop; a foreign
  // thread is a runtime-contract violation checked in every build.
  coro::Task<base::Result<LUringStream>> Accept();

  [[nodiscard]]
  base::Result<LUringAcceptSource> AcceptSource(net::AcceptSourceOptions options = {}) noexcept;

  coro::Task<base::Result<void>> Close();

  [[nodiscard]]
  base::Result<net::Endpoint> LocalAddress() const noexcept;

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
