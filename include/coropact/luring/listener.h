// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/backend/accept_source.h"
#include "coropact/backend/async_listener.h"
#include "coropact/luring/detail/completion_dispatch.h"
#include "coropact/luring/op.h"
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
  // write algorithms. It is opt-in and does not change WriteSome() itself.
  bool zero_copy_writes{false};
  // Optional process-shared diagnostics sink for accepted streams. The sink
  // must outlive the listener and all accepted streams.
  ZeroCopySendDiagnostics* zero_copy_diagnostics{nullptr};
  int backlog{SOMAXCONN};
  // Number of accepts kept in flight by each worker. A value greater than one
  // prevents a connection burst from being serialized behind one accept CQE.
  std::size_t accept_depth{4};
};

class LUringAcceptSource {
  friend class LUringListener;

  friend void detail::DispatchAcceptSourceComplete(
      LUringOp* op,
      CompletionEvent event) noexcept;

  friend void detail::DispatchAcceptSourceCancelComplete(
      LUringOp* op) noexcept;

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

  class AcceptOperation final : public LUringOp {
  public:
    explicit AcceptOperation(LUringAcceptSource* source) noexcept
        : source_(source) {
      kind = LUringOpKind::kAcceptSourceComplete;
    }

    [[nodiscard]]
    LUringAcceptSource* Source() const noexcept {
      return source_;
    }

    void Prepare() noexcept {
      kind = LUringOpKind::kAcceptSourceComplete;
      BeginNextRequest();
    }

  private:
    LUringAcceptSource* source_;
  };

  class CancelOperation final : public LUringOp {
  public:
    explicit CancelOperation(LUringAcceptSource* source) noexcept
        : source_(source) {
      kind = LUringOpKind::kAcceptSourceCancelComplete;
    }

    [[nodiscard]]
    LUringAcceptSource* Source() const noexcept {
      return source_;
    }

    void Prepare() noexcept {
      kind = LUringOpKind::kAcceptSourceCancelComplete;
      BeginNextRequest();
    }

  private:
    LUringAcceptSource* source_;
  };

  LUringAcceptSource(
      LUringListener* listener,
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
  void RequestBackendStop(
      std::optional<base::Error> error = std::nullopt) noexcept;

  void OnCompletion(CompletionEvent event) noexcept;
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
  friend void detail::DispatchAcceptComplete(LUringOp* op) noexcept;
  friend void detail::DispatchListenerCloseComplete(LUringOp* op) noexcept;

public:
  COROPACT_DELETE_COPY(LUringListener);

  using Stream = LUringStream;

  static base::Result<LUringListener> Create(
      LUringLoop* loop,
      const net::Endpoint& listen_addr,
      LUringListenOptions options = {}) noexcept;

  ~LUringListener();

  LUringListener(LUringListener&& other) noexcept;
  LUringListener& operator=(LUringListener&& other) noexcept;

  coro::Task<base::Result<LUringStream>> Accept();

  [[nodiscard]]
  base::Result<LUringAcceptSource> AcceptSource(
      net::AcceptSourceOptions options = {}) noexcept;

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
  LUringListener(LUringLoop* loop, int fd, bool zero_copy_writes,
                 ZeroCopySendDiagnostics* zero_copy_diagnostics) noexcept;

  void NotifyCloseProgress() noexcept;
  void ResetForMove() noexcept;
  static LUringLoop* PrepareMove(LUringListener& other) noexcept;

  LUringLoop* loop_;
  int fd_{-1};
  std::size_t pending_accepts_{0};
  CloseAwaiter* pending_close_{nullptr};
  LUringAcceptSource* accept_source_{nullptr};
  bool zero_copy_writes_{false};
  ZeroCopySendDiagnostics* zero_copy_diagnostics_{nullptr};
  bool closed_{false};
};

static_assert(backend::AsyncListener<LUringListener>);

}  // namespace coropact::luring
