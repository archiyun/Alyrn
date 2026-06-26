// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/socket.h"

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>

#include "vexo/log/logger.h"
#include "vexo/net/net_utils.h"

namespace vexo::net {

Socket::Socket(int sockfd) : sockfd_(sockfd) {}

Socket::~Socket() { ::close(sockfd_); }

void Socket::BindAddress(const InetAddress& localAddr) {
  const int ret = ::bind(sockfd_, reinterpret_cast<const sockaddr*>(&localAddr.sock_addr()),
                         static_cast<socklen_t>(sizeof(sockaddr_in)));
  if (ret == 0) {
    return;
  }

  LOG_FATAL() << "bind failed: fd=" << sockfd_ << " address=" << localAddr.ToIpPort()
              << " errno=" << errno << " message=" << std::strerror(errno);
  std::abort();
}

void Socket::Listen() {
  const int ret = ::listen(sockfd_, SOMAXCONN);
  if (ret == 0) {
    return;
  }

  LOG_FATAL() << "listen failed: fd=" << sockfd_ << " errno=" << errno
              << " message=" << std::strerror(errno);
  std::abort();
}

int Socket::Accept(InetAddress* peeraddr) {
  sockaddr_in addr{};
  socklen_t len = static_cast<socklen_t>(sizeof(addr));

  int connfd =
      ::accept4(sockfd_, reinterpret_cast<sockaddr*>(&addr), &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
  if (connfd >= 0 && peeraddr) {
    *peeraddr = InetAddress(addr);
  } else if (connfd < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
    LOG_ERROR() << "accept failed: listen_fd=" << sockfd_ << " errno=" << errno
                << " message=" << std::strerror(errno);
  }

  return connfd;
}

void Socket::ShutdownWrite() {
  if (::shutdown(sockfd_, SHUT_WR) == 0) {
    return;
  }

  LOG_ERROR() << "shutdown write failed: fd=" << sockfd_ << " errno=" << errno
              << " message=" << std::strerror(errno);
}

void Socket::set_tcp_no_delay(bool on) {
  const auto result = vexo::net::set_tcp_non_delay(sockfd_, on);
  if (result) {
    return;
  }

  LOG_ERROR() << "setsockopt TCP_NODELAY failed: fd=" << sockfd_ << " on=" << on
              << " error=" << result.error().value() << " message=" << result.error().message();
}

void Socket::set_reuse_addr(bool on) {
  const auto result = vexo::net::set_reuse_addr(sockfd_, on);
  if (result) {
    return;
  }

  LOG_ERROR() << "setsockopt SO_REUSEADDR failed: fd=" << sockfd_ << " on=" << on
              << " error=" << result.error().value() << " message=" << result.error().message();
}

void Socket::set_reuse_port(bool on) {
  const auto result = vexo::net::set_reuse_port(sockfd_, on);
  if (result) {
    return;
  }

  LOG_ERROR() << "setsockopt SO_REUSEPORT failed: fd=" << sockfd_ << " on=" << on
              << " error=" << result.error().value() << " message=" << result.error().message();
}

void Socket::set_keep_alive(bool on) {
  const auto result = vexo::net::set_keep_alive(sockfd_, on);
  if (result) {
    return;
  }

  LOG_ERROR() << "setsockopt SO_KEEPALIVE failed: fd=" << sockfd_ << " on=" << on
              << " error=" << result.error().value() << " message=" << result.error().message();
}

}  // namespace vexo::net
