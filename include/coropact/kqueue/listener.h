// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <deque>
#include <optional>

#include "coropact/backend/accept_source.h"
#include "coropact/backend/detail/value_result_state.h"
#include "coropact/result.h"
#include "coropact/coro/task.h"
#include "coropact/net/accept_source.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/socket.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/kqueue/detail/channel.h"
#include "coropact/kqueue/detail/loop_shutdown.h"
#include "coropact/kqueue/loop.h"
#include "coropact/kqueue/stream.h"
#include "coropact/utils/macros.h"

namespace coropact::kqueue {

class KqueueListener;

class KqueueAcceptSource {
public:
  COROPACT_DELETE_COPY(KqueueAcceptSource);

  using Stream = KqueueStream;
  using Event = std::optional<Stream>;
  using NextResult = coropact::Result<Event>;

  // Direct awaiter for the single-consumer accept loop. It keeps Next() on
  // the caller's coroutine frame and avoids a child Task frame per accepted
  // connection, while the source retains admission and terminal state.
  class NextAwaiter {
  public:
    explicit NextAwaiter(KqueueAcceptSource& source) noexcept : source_(&source) {}

    [[nodiscard]]
    bool await_ready() const noexcept {
      return false;
    }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept;

    NextResult await_resume() noexcept;

    void Complete(NextResult result) noexcept;

  private:
    KqueueAcceptSource* source_;
    operation::detail::SchedulerContinuation continuation_;
    operation::detail::CompletionGate completion_gate_;
    backend::detail::ValueResultState<Event> result_;
  };

  ~KqueueAcceptSource();

  KqueueAcceptSource(KqueueAcceptSource&& other) noexcept;
  KqueueAcceptSource& operator=(KqueueAcceptSource&& other) noexcept;

  [[nodiscard]]
  NextAwaiter Next() noexcept {
    return NextAwaiter(*this);
  }
  coro::Task<Result<void>> Stop();

private:
  friend class KqueueListener;

  KqueueAcceptSource(KqueueListener* listener,
                      net::detail::AcceptSourceStateMachine state) noexcept;

  void OnReady() noexcept;
  void OnError(Error error) noexcept;
  void OnListenerClosed() noexcept;
  void EnsureAdmission() noexcept;
  void DeliverNextIfReady() noexcept;
  bool TryTakeNext(NextResult& result) noexcept;
  void ReleaseListenerReservation() noexcept;
  void Fail(Error error) noexcept;
  Result<KqueueStream> TryAccept() noexcept;

  KqueueListener* listener_{nullptr};
  net::detail::AcceptSourceStateMachine state_;
  std::deque<KqueueStream> events_;
  std::optional<Error> terminal_error_;
  NextAwaiter* pending_next_{nullptr};
};

static_assert(backend::AsyncAcceptSource<KqueueAcceptSource>);

struct KqueueListenerOptions {
  bool reuse_addr{true};
  bool reuse_port{false};
  // Applies to every KqueueStream returned by Accept and AcceptSource.
  KqueueStreamOptions stream_options{};
};

class KqueueListener {
public:
  COROPACT_DELETE_COPY(KqueueListener);

  using Stream = KqueueStream;

  [[nodiscard]]
  static Result<KqueueListener> Create(KqueueLoop* loop, const net::Endpoint& listen_addr,
                                              KqueueListenerOptions options = {}) noexcept;

  KqueueListener(KqueueLoop* loop, const net::Endpoint& listen_addr,
                  KqueueListenerOptions options = {});
  ~KqueueListener();

  // Moves are loop-affine: the source must be used from its owning loop
  // thread and must not have a pending accept operation.
  KqueueListener(KqueueListener&& other) noexcept;
  KqueueListener& operator=(KqueueListener&& other) noexcept;

  coro::Task<Result<KqueueStream>> Accept();
  [[nodiscard]]
  Result<KqueueAcceptSource> AcceptSource(net::AcceptSourceOptions options = {}) noexcept;
  coro::Task<Result<void>> Close();

  // Accept, Close, AcceptSource, and destruction are loop-affine. The caller
  // must use this listener from its owning KqueueLoop thread; a foreign thread
  // is a runtime-contract violation checked in every build configuration.

  [[nodiscard]]
  Result<net::Endpoint> LocalAddress() const;

private:
  friend class KqueueAcceptSource;

  class AcceptAwaiter;

  KqueueListener(KqueueLoop* loop, net::Socket socket,
                  KqueueStreamOptions stream_options) noexcept;

  void HandleRead();
  void HandleError();
  static void DispatchRead(void* context) noexcept;
  static void DispatchError(void* context) noexcept;
  void CompleteAccept(Result<KqueueStream> result);
  void CloseNow() noexcept;
  void DetachChannel();
  void RequireOwnerLoop() const noexcept;
  void BindChannelCallbacks() noexcept;
  void ResetForMove() noexcept;
  static KqueueLoop* PrepareMove(KqueueListener& other) noexcept;
  static void DispatchLoopStop(void* context) noexcept;

  KqueueLoop* loop_;
  net::Socket socket_;
  detail::Channel channel_;
  KqueueStreamOptions stream_options_;
  AcceptAwaiter* pending_accept_{nullptr};
  KqueueAcceptSource* accept_source_{nullptr};
  bool closed_{false};
  detail::LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

}  // namespace coropact::kqueue
