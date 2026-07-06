// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <memory>

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
  VEXO_DELETE_COPY_MOVE(ReactorListener);
  using Stream = ReactorStream;

  ReactorListener(EventLoop* loop, const InetAddress& listen_addr);
  ~ReactorListener();

  coro::Task<base::Result<std::unique_ptr<ReactorStream>>> Accept();
  coro::Task<base::Result<void>> Close();

  base::Result<InetAddress> LocalAddress() const;

private:
  class AcceptAwaiter;

  void HandleRead(vexo::time::Timestamp receive_time);
  void HandleError();
  void CompleteAccept(base::Result<std::unique_ptr<ReactorStream>> result);
  void DetachChannel();

  EventLoop* loop_;
  Socket socket_;
  Channel channel_;
  AcceptAwaiter* pending_accept_{nullptr};
  bool closed_{false};
};

}  // namespace vexo::net
