// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/net/net_utils.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include <expected>
#include <string>

namespace coropact::net {
namespace {

coropact::base::Result<void> set_fd_flag(int fd, int cmd_get, int cmd_set, int flag, bool on) {
  const int old_flag = ::fcntl(fd, cmd_get, 0);
  if (old_flag < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  const int new_flag = on ? (old_flag | flag) : (old_flag & ~flag);
  if (new_flag != old_flag && ::fcntl(fd, cmd_set, new_flag) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  return {};
}

coropact::base::Result<void> set_socket_option(int fd, int level, int option, bool on) {
  const int optval = on ? 1 : 0;
  if (::setsockopt(fd, level, option, &optval, static_cast<socklen_t>(sizeof(optval))) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }
  return {};
}

}  // namespace

coropact::base::Result<Endpoint> ParseIpAddress(std::string_view ip, std::uint16_t port) {
  if (ip.find('\0') != std::string_view::npos) {
    return std::unexpected(coropact::base::Error(std::make_error_code(std::errc::invalid_argument)));
  }

  const std::string ip_string(ip);
  sockaddr_in ipv4{};
  ipv4.sin_family = AF_INET;
  ipv4.sin_port = htons(port);
  if (::inet_pton(AF_INET, ip_string.c_str(), &ipv4.sin_addr) == 1) {
    return Endpoint(ipv4);
  }

  sockaddr_in6 ipv6{};
  ipv6.sin6_family = AF_INET6;
  ipv6.sin6_port = htons(port);
  if (::inet_pton(AF_INET6, ip_string.c_str(), &ipv6.sin6_addr) == 1) {
    return Endpoint(ipv6);
  }

  return std::unexpected(
      coropact::base::Error(std::make_error_code(std::errc::invalid_argument)));
}

coropact::base::Result<int> CreateNonBlockingSocket(sa_family_t family) {
  if (family != AF_INET && family != AF_INET6) {
    return std::unexpected(
        coropact::base::Error(std::make_error_code(std::errc::address_family_not_supported)));
  }

  const int sockfd = ::socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (sockfd < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }
  return sockfd;
}

coropact::base::Result<void> SetNonBlocking(int fd, bool on) {
  return set_fd_flag(fd, F_GETFL, F_SETFL, O_NONBLOCK, on);
}

coropact::base::Result<void> SetCloseOnExec(int fd, bool on) {
  return set_fd_flag(fd, F_GETFD, F_SETFD, FD_CLOEXEC, on);
}

coropact::base::Result<void> SetReuseAddr(int fd, bool on) {
  return set_socket_option(fd, SOL_SOCKET, SO_REUSEADDR, on);
}

coropact::base::Result<void> SetReusePort(int fd, bool on) {
  return set_socket_option(fd, SOL_SOCKET, SO_REUSEPORT, on);
}

coropact::base::Result<void> SetTcpNonDelay(int fd, bool on) {
  return set_socket_option(fd, IPPROTO_TCP, TCP_NODELAY, on);
}

coropact::base::Result<void> SetKeepAlive(int fd, bool on) {
  return set_socket_option(fd, SOL_SOCKET, SO_KEEPALIVE, on);
}

coropact::base::Result<Endpoint> GetLocalAddr(int fd) {
  sockaddr_storage localaddr{};
  socklen_t addrlen = static_cast<socklen_t>(sizeof(localaddr));
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&localaddr), &addrlen) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }
  if (localaddr.ss_family != AF_INET && localaddr.ss_family != AF_INET6) {
    return std::unexpected(
        coropact::base::Error(std::make_error_code(std::errc::address_family_not_supported)));
  }
  return Endpoint(reinterpret_cast<const sockaddr*>(&localaddr), addrlen);
}

coropact::base::Result<Endpoint> GetPeerAddr(int fd) {
  sockaddr_storage peeraddr{};
  socklen_t addrlen = static_cast<socklen_t>(sizeof(peeraddr));
  if (::getpeername(fd, reinterpret_cast<sockaddr*>(&peeraddr), &addrlen) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }
  if (peeraddr.ss_family != AF_INET && peeraddr.ss_family != AF_INET6) {
    return std::unexpected(
        coropact::base::Error(std::make_error_code(std::errc::address_family_not_supported)));
  }
  return Endpoint(reinterpret_cast<const sockaddr*>(&peeraddr), addrlen);
}

coropact::base::Result<bool> IsSelfConnect(int fd) {
  auto localaddr = GetLocalAddr(fd);
  if (!localaddr) return std::unexpected(localaddr.error());
  auto peeraddr = GetPeerAddr(fd);
  if (!peeraddr) return std::unexpected(peeraddr.error());
  return *localaddr == *peeraddr;
}

}  // namespace coropact::net
