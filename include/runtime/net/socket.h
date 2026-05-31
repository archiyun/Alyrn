// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/net/inet_address.h"

namespace runtime::net {

// Socket is an RAII wrapper around a socket file descriptor.
//
// It owns the descriptor for its lifetime and exposes a small set of socket
// operations used by the networking layer.
class Socket : public runtime::base::NonCopyable {
public:
  explicit Socket(int sockfd);
  ~Socket();

  int fd() const { return sockfd_; }

  // Binds the socket to a local address.
  void BindAddress(const InetAddress& localaddr);

  // Marks the socket as a passive listening socket.
  void Listen();

  // Accepts a new inbound connection and optionally fills the peer address.
  int Accept(InetAddress* peeraddr);

  // Shuts down the write side of the socket.
  void ShutdownWrite();

  // Enables or disables TCP_NODELAY.
  void set_tcp_no_delay(bool on);

  // Enables or disables SO_REUSEADDR.
  void set_reuse_addr(bool on);

  // Enables or disables SO_REUSEPORT.
  void set_reuse_port(bool on);

  // Enables or disables SO_KEEPALIVE.
  void set_keep_alive(bool on);

private:
  const int sockfd_;
};

}  // namespace runtime::net
