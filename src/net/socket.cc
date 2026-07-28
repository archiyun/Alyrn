// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/net/socket.h"

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "coropact/base/check.h"
#include "coropact/log/logger.h"
#include "coropact/net/net_utils.h"

namespace coropact::net {

Socket::Socket(int sockfd) : sockfd_(sockfd) {}

Socket::~Socket() { Close(); }

void Socket::BindAddress(const Endpoint& localAddr) {
  const int ret = ::bind(sockfd_, localAddr.SockAddr(), localAddr.SockAddrLen());
  if (ret == 0) {
    return;
  }

  LOG_FATALF("bind failed: fd={} address={} errno={} message={}", sockfd_, localAddr.ToIpPort(),
             errno, std::strerror(errno));
  COROPACT_CHECK(false, "Socket::BindAddress: bind failed");
}

void Socket::Listen() {
  const int ret = ::listen(sockfd_, SOMAXCONN);
  if (ret == 0) {
    return;
  }

  LOG_FATALF("listen failed: fd={} errno={} message={}", sockfd_, errno, std::strerror(errno));
  COROPACT_CHECK(false, "Socket::Listen: listen failed");
}

int Socket::Accept(Endpoint* peeraddr) {
  sockaddr_storage addr{};
  auto len = static_cast<socklen_t>(sizeof(addr));

  int connfd =
      ::accept4(sockfd_, reinterpret_cast<sockaddr*>(&addr), &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
  if (connfd >= 0 && peeraddr != nullptr) {
    *peeraddr = Endpoint(reinterpret_cast<const sockaddr*>(&addr), len);
  } else if (connfd < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
    LOG_ERRORF("accept failed: listen_fd={} errno={} message={}", sockfd_, errno,
               std::strerror(errno));
  }

  return connfd;
}

void Socket::ShutdownWrite() {
  if (sockfd_ < 0) {
    return;
  }

  if (::shutdown(sockfd_, SHUT_WR) == 0) {
    return;
  }

  LOG_ERRORF("shutdown write failed: fd={} errno={} message={}", sockfd_, errno,
             std::strerror(errno));
}

void Socket::Close() noexcept {
  if (sockfd_ < 0) {
    return;
  }

  const int fd = sockfd_;
  sockfd_ = -1;
  if (::close(fd) == 0) {
    return;
  }

  LOG_ERRORF("socket close failed: fd={} errno={} message={}", fd, errno, std::strerror(errno));
}

void Socket::SetTcpNoDelay(bool on) {
  const auto result = coropact::net::SetTcpNonDelay(sockfd_, on);
  if (result) {
    return;
  }

  LOG_ERRORF("setsockopt TCP_NODELAY failed: fd={} on={} error={} message={}", sockfd_, on ? 1 : 0,
             result.error().value(), result.error().message());
}

void Socket::SetReuseAddr(bool on) {
  const auto result = coropact::net::SetReuseAddr(sockfd_, on);
  if (result) {
    return;
  }

  LOG_ERRORF("setsockopt SO_REUSEADDR failed: fd={} on={} error={} message={}", sockfd_, on ? 1 : 0,
             result.error().value(), result.error().message());
}

void Socket::SetReusePort(bool on) {
  const auto result = coropact::net::SetReusePort(sockfd_, on);
  if (result) {
    return;
  }

  LOG_ERRORF("setsockopt SO_REUSEPORT failed: fd={} on={} error={} message={}", sockfd_, on ? 1 : 0,
             result.error().value(), result.error().message());
}

void Socket::SetKeepAlive(bool on) {
  const auto result = coropact::net::SetKeepAlive(sockfd_, on);
  if (result) {
    return;
  }

  LOG_ERRORF("setsockopt SO_KEEPALIVE failed: fd={} on={} error={} message={}", sockfd_, on ? 1 : 0,
             result.error().value(), result.error().message());
}

}  // namespace coropact::net
