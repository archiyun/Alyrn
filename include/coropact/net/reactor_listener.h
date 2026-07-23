// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/net/channel.h"
#include "coropact/net/event_loop.h"
#include "coropact/net/inet_address.h"
#include "coropact/net/reactor_stream.h"
#include "coropact/net/socket.h"
#include "coropact/time/timestamp.h"
#include "coropact/utils/macros.h"

namespace coropact::net {

struct ReactorListenerOptions {
  bool reuse_addr{true};
  bool reuse_port{false};
};

class ReactorListener {
public:
  COROPACT_DELETE_COPY(ReactorListener);

  using Stream = ReactorStream;

  [[nodiscard]] static base::Result<ReactorListener> Create(
      EventLoop* loop, const InetAddress& listen_addr,
      ReactorListenerOptions options = {}) noexcept;

  ReactorListener(EventLoop* loop, const InetAddress& listen_addr,
                  ReactorListenerOptions options = {});
  ~ReactorListener();

  // Moves are loop-affine: the source must be used from its owning loop
  // thread and must not have a pending accept operation.
  ReactorListener(ReactorListener&& other) noexcept;
  ReactorListener& operator=(ReactorListener&& other) noexcept;

  coro::Task<base::Result<ReactorStream>> Accept();
  coro::Task<base::Result<void>> Close();

  [[nodiscard]] base::Result<InetAddress> LocalAddress() const;

private:
  class AcceptAwaiter;

  ReactorListener(EventLoop* loop, Socket socket) noexcept;

  void HandleRead(coropact::time::Timestamp receive_time);
  void HandleError();
  void CompleteAccept(base::Result<ReactorStream> result);
  void DetachChannel();
  void BindChannelCallbacks() noexcept;
  void ResetForMove() noexcept;
  static EventLoop* PrepareMove(ReactorListener& other) noexcept;

  EventLoop* loop_;
  Socket socket_;
  Channel channel_;
  AcceptAwaiter* pending_accept_{nullptr};
  bool closed_{false};
};

}  // namespace coropact::net
