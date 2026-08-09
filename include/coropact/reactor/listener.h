// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <deque>
#include <optional>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/net/accept_source.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/socket.h"
#include "coropact/reactor/detail/channel.h"
#include "coropact/reactor/detail/loop_shutdown.h"
#include "coropact/reactor/loop.h"
#include "coropact/reactor/stream.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

class ReactorListener;

class ReactorAcceptSource {
public:
  COROPACT_DELETE_COPY(ReactorAcceptSource);

  using Stream = ReactorStream;
  using Event = std::optional<Stream>;
  using Result = base::Result<Event>;

  ~ReactorAcceptSource();

  ReactorAcceptSource(ReactorAcceptSource&& other) noexcept;
  ReactorAcceptSource& operator=(ReactorAcceptSource&& other) noexcept;

  coro::Task<Result> Next();
  coro::Task<base::Result<void>> Stop();

private:
  friend class ReactorListener;

  class NextAwaiter;

  ReactorAcceptSource(ReactorListener* listener,
                      net::detail::AcceptSourceStateMachine state) noexcept;

  void OnReady() noexcept;
  void OnError(base::Error error) noexcept;
  void OnListenerClosed() noexcept;
  void EnsureAdmission() noexcept;
  void DeliverNextIfReady() noexcept;
  bool TryTakeNext(Result& result) noexcept;
  void ReleaseListenerReservation() noexcept;
  void Fail(base::Error error) noexcept;
  base::Result<ReactorStream> TryAccept() noexcept;

  ReactorListener* listener_{nullptr};
  net::detail::AcceptSourceStateMachine state_;
  std::deque<ReactorStream> events_;
  std::optional<base::Error> terminal_error_;
  NextAwaiter* pending_next_{nullptr};
};

struct ReactorListenerOptions {
  bool reuse_addr{true};
  bool reuse_port{false};
  // Applies to every ReactorStream returned by Accept and AcceptSource.
  ReactorStreamOptions stream_options{};
};

class ReactorListener {
public:
  COROPACT_DELETE_COPY(ReactorListener);

  using Stream = ReactorStream;

  [[nodiscard]]
  static base::Result<ReactorListener> Create(EventLoop* loop, const net::Endpoint& listen_addr,
                                              ReactorListenerOptions options = {}) noexcept;

  ReactorListener(EventLoop* loop, const net::Endpoint& listen_addr,
                  ReactorListenerOptions options = {});
  ~ReactorListener();

  // Moves are loop-affine: the source must be used from its owning loop
  // thread and must not have a pending accept operation.
  ReactorListener(ReactorListener&& other) noexcept;
  ReactorListener& operator=(ReactorListener&& other) noexcept;

  coro::Task<base::Result<ReactorStream>> Accept();
  [[nodiscard]]
  base::Result<ReactorAcceptSource> AcceptSource(net::AcceptSourceOptions options = {}) noexcept;
  coro::Task<base::Result<void>> Close();

  // Accept, Close, AcceptSource, and destruction are loop-affine. The caller
  // must use this listener from its owning EventLoop thread; a foreign thread
  // is a runtime-contract violation checked in every build configuration.

  [[nodiscard]]
  base::Result<net::Endpoint> LocalAddress() const;

private:
  friend class ReactorAcceptSource;

  class AcceptAwaiter;

  ReactorListener(EventLoop* loop, net::Socket socket,
                  ReactorStreamOptions stream_options) noexcept;

  void HandleRead();
  void HandleError();
  static void DispatchRead(void* context) noexcept;
  static void DispatchError(void* context) noexcept;
  void CompleteAccept(base::Result<ReactorStream> result);
  void CloseNow() noexcept;
  void DetachChannel();
  void RequireOwnerLoop() const noexcept;
  void BindChannelCallbacks() noexcept;
  void ResetForMove() noexcept;
  static EventLoop* PrepareMove(ReactorListener& other) noexcept;
  static void DispatchLoopStop(void* context) noexcept;

  EventLoop* loop_;
  net::Socket socket_;
  detail::Channel channel_;
  ReactorStreamOptions stream_options_;
  AcceptAwaiter* pending_accept_{nullptr};
  ReactorAcceptSource* accept_source_{nullptr};
  bool closed_{false};
  detail::LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

}  // namespace coropact::reactor
