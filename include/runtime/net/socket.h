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

  int Fd() const { return sockfd_; }

  // Binds the socket to a local address.
  void BindAddress(const InetAddress& localaddr);

  // Marks the socket as a passive listening socket.
  void Listen();

  // Accepts a new inbound connection and optionally fills the peer address.
  int Accept(InetAddress* peeraddr);

  // Shuts down the write side of the socket.
  void ShutdownWrite();

  // Enables or disables TCP_NODELAY.
  void SetTcpNoDelay(bool on);

  // Enables or disables SO_REUSEADDR.
  void SetReuseAddr(bool on);

  // Enables or disables SO_REUSEPORT.
  void SetReusePort(bool on);

  // Enables or disables SO_KEEPALIVE.
  void SetKeepAlive(bool on);

private:
  const int sockfd_;
};

}  // namespace runtime::net
