// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <functional>

#include "vexo/net/channel.h"
#include "vexo/net/socket.h"
#include "vexo/utils/macros.h"

namespace vexo::net {

class EventLoop;
class InetAddress;

// Acceptor owns the listening socket and accepts new inbound TCP connections.
//
// It is typically attached to the base EventLoop. When the listening fd
// becomes readable, Acceptor accepts one or more pending connections and
// forwards them through NewConnectionCallback.
class Acceptor {
public:
  using NewConnectionCallback = std::function<void(int sockfd, const InetAddress&)>;

  Acceptor(EventLoop* loop, const InetAddress& listen_addr, bool reuse_port);
  ~Acceptor();

  VEXO_DELETE_COPY_MOVE(Acceptor);

  void set_new_connection_callback(NewConnectionCallback cb) {
    new_connection_callback_ = std::move(cb);
  }

  // Must be called before Listen() so accept_channel_ is registered with EPOLLET.
  void set_edge_triggered(bool et) { accept_channel_.set_edge_triggered(et); }

  bool listening() const { return listening_; }
  void Listen();

private:
  void HandleRead(vexo::time::Timestamp receive_time);

  EventLoop* loop_;
  Socket accept_socket_;
  Channel accept_channel_;
  NewConnectionCallback new_connection_callback_;
  bool listening_;
};

}  // namespace vexo::net
