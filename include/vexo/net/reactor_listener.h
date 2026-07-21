// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/net/channel.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/reactor_stream.h"
#include "vexo/net/socket.h"
#include "vexo/time/timestamp.h"
#include "vexo/utils/macros.h"

namespace vexo::net {

class ReactorListener {
public:
  VEXO_DELETE_COPY(ReactorListener);

  using Stream = ReactorStream;

  ReactorListener(EventLoop* loop, const InetAddress& listen_addr);
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

  void HandleRead(vexo::time::Timestamp receive_time);
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

}  // namespace vexo::net
