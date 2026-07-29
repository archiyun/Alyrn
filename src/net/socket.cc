// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/net/socket.h"

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "coropact/base/check.h"
#include "coropact/net/net_utils.h"

namespace coropact::net {

Socket::Socket(int sockfd) : sockfd_(sockfd) {}

Socket::~Socket() { Close(); }

void Socket::BindAddress(const Endpoint& localAddr) {
  const int ret = ::bind(sockfd_, localAddr.SockAddr(), localAddr.SockAddrLen());
  COROPACT_CHECK(ret == 0, "Socket::BindAddress: bind failed");
}

void Socket::Listen() {
  const int ret = ::listen(sockfd_, SOMAXCONN);
  COROPACT_CHECK(ret == 0, "Socket::Listen: listen failed");
}

int Socket::Accept(Endpoint* peeraddr) {
  sockaddr_storage addr{};
  auto len = static_cast<socklen_t>(sizeof(addr));

  int connfd =
      ::accept4(sockfd_, reinterpret_cast<sockaddr*>(&addr), &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
  if (connfd >= 0 && peeraddr != nullptr) {
    *peeraddr = Endpoint(reinterpret_cast<const sockaddr*>(&addr), len);
  }

  return connfd;
}

void Socket::ShutdownWrite() {
  if (sockfd_ < 0) {
    return;
  }

  (void)::shutdown(sockfd_, SHUT_WR);
}

void Socket::Close() noexcept {
  if (sockfd_ < 0) {
    return;
  }

  const int fd = sockfd_;
  sockfd_ = -1;
  (void)::close(fd);
}

void Socket::SetTcpNoDelay(bool on) {
  const auto result = coropact::net::SetTcpNonDelay(sockfd_, on);
  COROPACT_CHECK(result.has_value(), "Socket::SetTcpNoDelay: setsockopt failed");
}

void Socket::SetReuseAddr(bool on) {
  const auto result = coropact::net::SetReuseAddr(sockfd_, on);
  COROPACT_CHECK(result.has_value(), "Socket::SetReuseAddr: setsockopt failed");
}

void Socket::SetReusePort(bool on) {
  const auto result = coropact::net::SetReusePort(sockfd_, on);
  COROPACT_CHECK(result.has_value(), "Socket::SetReusePort: setsockopt failed");
}

void Socket::SetKeepAlive(bool on) {
  const auto result = coropact::net::SetKeepAlive(sockfd_, on);
  COROPACT_CHECK(result.has_value(), "Socket::SetKeepAlive: setsockopt failed");
}

}  // namespace coropact::net
