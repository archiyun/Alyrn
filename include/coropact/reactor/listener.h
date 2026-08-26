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
#include "coropact/reactor/detail/channel.h"
#include "coropact/reactor/detail/loop_shutdown.h"
#include "coropact/reactor/loop.h"
#include "coropact/reactor/stream.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

class Listener;

class AcceptSource {
public:
  COROPACT_DELETE_COPY(AcceptSource);

  using StreamType = Stream;
  using Event = std::optional<Stream>;
  using NextResult = coropact::Result<Event>;

  // Direct awaiter for the single-consumer accept loop. It keeps Next() on
  // the caller's coroutine frame and avoids a child Task frame per accepted
  // connection, while the source retains admission and terminal state.
  class NextAwaiter {
  public:
    explicit NextAwaiter(AcceptSource& source) noexcept : source_(&source) {}

    [[nodiscard]]
    bool await_ready() const noexcept {
      return false;
    }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept;

    NextResult await_resume() noexcept;

    void Complete(NextResult result) noexcept;

  private:
    AcceptSource* source_;
    operation::detail::SchedulerContinuation continuation_;
    operation::detail::CompletionGate completion_gate_;
    backend::detail::ValueResultState<Event> result_;
  };

  ~AcceptSource();

  AcceptSource(AcceptSource&& other) noexcept;
  AcceptSource& operator=(AcceptSource&& other) noexcept;

  [[nodiscard]]
  NextAwaiter Next() noexcept {
    return NextAwaiter(*this);
  }
  coro::Task<Result<void>> Stop();

private:
  friend class Listener;

  AcceptSource(Listener* listener,
                      net::detail::AcceptSourceStateMachine state) noexcept;

  void OnReady() noexcept;
  void OnError(Error error) noexcept;
  void OnListenerClosed() noexcept;
  void EnsureAdmission() noexcept;
  void DeliverNextIfReady() noexcept;
  bool TryTakeNext(NextResult& result) noexcept;
  void ReleaseListenerReservation() noexcept;
  void Fail(Error error) noexcept;
  Result<Stream> TryAccept() noexcept;

  Listener* listener_{nullptr};
  net::detail::AcceptSourceStateMachine state_;
  std::deque<Stream> events_;
  std::optional<Error> terminal_error_;
  NextAwaiter* pending_next_{nullptr};
};

static_assert(backend::AsyncAcceptSource<AcceptSource>);

struct ListenerOptions {
  bool reuse_addr{true};
  bool reuse_port{false};
  // Applies to every Stream returned by Accept and AcceptSource.
  StreamOptions stream_options{};
};

class Listener {
public:
  COROPACT_DELETE_COPY(Listener);

  using StreamType = Stream;

  [[nodiscard]]
  static Result<Listener> Create(Loop* loop, const net::Endpoint& listen_addr,
                                              ListenerOptions options = {}) noexcept;

  Listener(Loop* loop, const net::Endpoint& listen_addr,
                  ListenerOptions options = {});
  ~Listener();

  // Moves are loop-affine: the source must be used from its owning loop
  // thread and must not have a pending accept operation.
  Listener(Listener&& other) noexcept;
  Listener& operator=(Listener&& other) noexcept;

  coro::Task<Result<Stream>> Accept();
  [[nodiscard]]
  Result<AcceptSource> CreateAcceptSource(net::AcceptSourceOptions options = {}) noexcept;
  coro::Task<Result<void>> Close();

  // Accept, Close, CreateAcceptSource, and destruction are loop-affine. The caller
  // must use this listener from its owning Loop thread; a foreign thread
  // is a runtime-contract violation checked in every build configuration.

  [[nodiscard]]
  Result<net::Endpoint> LocalAddress() const;

private:
  friend class AcceptSource;

  class AcceptAwaiter;

  Listener(Loop* loop, net::Socket socket,
                  StreamOptions stream_options) noexcept;

  void HandleRead();
  void HandleError();
  static void DispatchRead(void* context) noexcept;
  static void DispatchError(void* context) noexcept;
  void CompleteAccept(Result<Stream> result);
  void CloseNow() noexcept;
  void DetachChannel();
  void RequireOwnerLoop() const noexcept;
  void BindChannelCallbacks() noexcept;
  void ResetForMove() noexcept;
  static Loop* PrepareMove(Listener& other) noexcept;
  static void DispatchLoopStop(void* context) noexcept;

  Loop* loop_;
  net::Socket socket_;
  detail::Channel channel_;
  StreamOptions stream_options_;
  AcceptAwaiter* pending_accept_{nullptr};
  AcceptSource* accept_source_{nullptr};
  bool closed_{false};
  detail::LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

}  // namespace coropact::reactor
