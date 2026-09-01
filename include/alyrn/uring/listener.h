// SPDX-License-Identifier: MIT
#pragma once

#include <sys/socket.h>

#include <coroutine>
#include <cstddef>
#include <deque>
#include <optional>

#include "alyrn/backend/accept_source.h"
#include "alyrn/backend/async_listener.h"
#include "alyrn/backend/value_result_state.h"
#include "alyrn/detail/completion_gate.h"
#include "alyrn/detail/macros.h"
#include "alyrn/detail/scheduler_continuation.h"
#include "alyrn/net/accept_source.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/net/tcp_options.h"
#include "alyrn/result.h"
#include "alyrn/task.h"
#include "alyrn/uring/detail/completion_dispatch.h"
#include "alyrn/uring/detail/op.h"
#include "alyrn/uring/stream.h"

namespace alyrn::uring {

class Loop;
class Listener;

struct ListenOptions {
  bool reuse_addr{true};
  bool reuse_port{true};
  // Exposes the uring stream's explicit zero-copy send extension to generic
  // Write(). It is opt-in; the physical single-send operation remains
  // internal to Stream.
  bool zero_copy_writes{false};
  int backlog{SOMAXCONN};
  // Number of accepts kept in flight by each worker. A value greater than one
  // prevents a connection burst from being serialized behind one accept CQE.
  std::size_t accept_depth{4};
  net::TcpOptions tcp_options{};
};

class AcceptSource {
  friend class Listener;

  friend detail::CompletionDisposition detail::DispatchAcceptSourceComplete(
      detail::Op* op, detail::CompletionEvent event) noexcept;

  friend void detail::DispatchAcceptSourceCancelComplete(detail::Op* op) noexcept;

public:
  ALYRN_DELETE_COPY(AcceptSource);

  using StreamType = Stream;
  using Event = std::optional<Stream>;
  using NextResult = Result<Event>;

  // Direct awaiter for the single-consumer accept loop. It preserves the
  // source's multishot/one-shot physical implementation while avoiding a
  // child Task frame for each logical accept event.
  class [[nodiscard]] NextAwaiter {
  public:
    explicit NextAwaiter(AcceptSource& source) noexcept : source_(&source) {}

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> continuation) noexcept;
    NextResult await_resume() noexcept;

    void Complete(NextResult result) noexcept;

  private:
    AcceptSource* source_;
    ::alyrn::detail::SchedulerContinuation continuation_;
    ::alyrn::detail::CompletionGate completion_gate_;
    backend::ValueResultState<Event> result_;
  };

  ~AcceptSource();

  AcceptSource(AcceptSource&& other) noexcept;
  AcceptSource& operator=(AcceptSource&& other) noexcept;

  NextAwaiter Next() noexcept { return NextAwaiter(*this); }
  Task<Result<void>> Stop();

private:
  class StopAwaiter;

  class AcceptOperation final : public detail::Op {
  public:
    explicit AcceptOperation(AcceptSource* source) noexcept : source_(source) {
      kind = detail::OpKind::kAcceptSourceComplete;
    }

    AcceptSource* Source() const noexcept { return source_; }

    void Prepare() noexcept {
      kind = detail::OpKind::kAcceptSourceComplete;
      BeginNextRequest();
    }

  private:
    AcceptSource* source_;
  };

  class CancelOperation final : public detail::Op {
  public:
    explicit CancelOperation(AcceptSource* source) noexcept : source_(source) {
      kind = detail::OpKind::kAcceptSourceCancelComplete;
    }

    AcceptSource* Source() const noexcept { return source_; }

    void Prepare() noexcept {
      kind = detail::OpKind::kAcceptSourceCancelComplete;
      BeginNextRequest();
    }

  private:
    AcceptSource* source_;
  };

  AcceptSource(Listener* listener, net::detail::AcceptSourceStateMachine state) noexcept;

  Result<void> Start() noexcept;

  Result<void> StartOperation() noexcept;

  Result<void> StartCancel() noexcept;

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

  Result<Stream> MakeStream(int accepted_fd) noexcept;

  Listener* listener_{nullptr};
  net::detail::AcceptSourceStateMachine state_;
  std::deque<Stream> events_;
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

static_assert(backend::AsyncAcceptSource<AcceptSource>);

class Listener {
  friend class AcceptSource;
  friend void detail::DispatchAcceptComplete(detail::Op* op) noexcept;
  friend void detail::DispatchListenerCloseComplete(detail::Op* op) noexcept;

public:
  ALYRN_DELETE_COPY(Listener);

  using StreamType = Stream;

  static Result<Listener> Create(Loop* loop, const net::Endpoint& listen_addr,
                                 ListenOptions options = {}) noexcept;

  ~Listener();

  Listener(Listener&& other) noexcept;
  Listener& operator=(Listener&& other) noexcept;

  // Accept, Close, and CreateAcceptSource are loop-affine. Their coroutine or
  // factory call must execute on this listener's owner Loop; a foreign
  // thread is a runtime-contract violation checked in every build.
  Task<Result<Stream>> Accept();

  [[nodiscard]]
  Result<AcceptSource> CreateAcceptSource(net::AcceptSourceOptions options = {}) noexcept;

  Task<Result<void>> Close();

  [[nodiscard]]
  Result<net::Endpoint> LocalAddress() const noexcept;

  [[nodiscard]]
  int Fd() const noexcept {
    return fd_;
  }

private:
  class AcceptAwaiter;
  class CloseAwaiter;

  Listener(Loop* loop, int fd, bool zero_copy_writes, net::TcpOptions tcp_options) noexcept;

  void RequireOwnerLoop() const noexcept;
  void NotifyCloseProgress() noexcept;
  void ResetForMove() noexcept;
  static Loop* PrepareMove(Listener& other) noexcept;

  Loop* loop_;
  int fd_{-1};
  std::size_t pending_accepts_{0};
  CloseAwaiter* pending_close_{nullptr};
  AcceptSource* accept_source_{nullptr};
  bool zero_copy_writes_{false};
  net::TcpOptions tcp_options_{};
  bool closed_{false};
};

static_assert(backend::AsyncListener<Listener>);

}  // namespace alyrn::uring
