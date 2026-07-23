// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <utility>

#include "coropact/net/inet_address.h"
#include "coropact/utils/macros.h"

namespace coropact::net {

// Socket is an RAII wrapper around a socket file descriptor.
//
// It owns the descriptor for its lifetime and exposes a small set of socket
// operations used by the networking layer.
class Socket {
public:
  COROPACT_DELETE_COPY(Socket);

  explicit Socket(int sockfd);
  ~Socket();

  // Move assignment closes the descriptor currently owned by *this before
  // taking ownership from other.
  Socket(Socket&& other) noexcept : sockfd_(std::exchange(other.sockfd_, -1)) {}
  Socket& operator=(Socket&& other) noexcept {
    if (this != &other) {
      Close();
      sockfd_ = std::exchange(other.sockfd_, -1);
    }
    return *this;
  }

  [[nodiscard]] int fd() const { return sockfd_; }

  // Binds the socket to a local address.
  void BindAddress(const InetAddress& localaddr);

  // Marks the socket as a passive listening socket.
  void Listen();

  // Accepts a new inbound connection and optionally fills the peer address.
  int Accept(InetAddress* peeraddr);

  // Shuts down the write side of the socket.
  void ShutdownWrite();

  // Closes the descriptor before destruction. Idempotent.
  void Close() noexcept;

  // Enables or disables TCP_NODELAY.
  void set_tcp_no_delay(bool on);

  // Enables or disables SO_REUSEADDR.
  void set_reuse_addr(bool on);

  // Enables or disables SO_REUSEPORT.
  void set_reuse_port(bool on);

  // Enables or disables SO_KEEPALIVE.
  void set_keep_alive(bool on);

private:
  int sockfd_;
};

}  // namespace coropact::net
