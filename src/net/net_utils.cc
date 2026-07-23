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

coropact::base::Result<InetAddress> ParseIPv4Address(std::string_view ip, std::uint16_t port) {
  if (ip.find('\0') != std::string_view::npos) {
    return std::unexpected(coropact::base::Error(std::make_error_code(std::errc::invalid_argument)));
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);

  const std::string ip_string(ip);
  const int result = ::inet_pton(AF_INET, ip_string.c_str(), &addr.sin_addr);
  if (result == 1) {
    return InetAddress(addr);
  }
  if (result == 0) {
    return std::unexpected(coropact::base::Error(std::make_error_code(std::errc::invalid_argument)));
  }
  return std::unexpected(coropact::base::CurrentErrno());
}

coropact::base::Result<int> CreateNonBlockingSocket() {
  const int sockfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (sockfd < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }
  return sockfd;
}

coropact::base::Result<void> set_non_blocking(int fd, bool on) {
  return set_fd_flag(fd, F_GETFL, F_SETFL, O_NONBLOCK, on);
}

coropact::base::Result<void> set_close_on_exec(int fd, bool on) {
  return set_fd_flag(fd, F_GETFD, F_SETFD, FD_CLOEXEC, on);
}

coropact::base::Result<void> set_reuse_addr(int fd, bool on) {
  return set_socket_option(fd, SOL_SOCKET, SO_REUSEADDR, on);
}

coropact::base::Result<void> set_reuse_port(int fd, bool on) {
  return set_socket_option(fd, SOL_SOCKET, SO_REUSEPORT, on);
}

coropact::base::Result<void> set_tcp_non_delay(int fd, bool on) {
  return set_socket_option(fd, IPPROTO_TCP, TCP_NODELAY, on);
}

coropact::base::Result<void> set_keep_alive(int fd, bool on) {
  return set_socket_option(fd, SOL_SOCKET, SO_KEEPALIVE, on);
}

coropact::base::Result<InetAddress> get_local_addr(int fd) {
  sockaddr_in localaddr{};
  socklen_t addrlen = static_cast<socklen_t>(sizeof(localaddr));
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&localaddr), &addrlen) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }
  if (localaddr.sin_family != AF_INET) {
    return std::unexpected(
        coropact::base::Error(std::make_error_code(std::errc::address_family_not_supported)));
  }
  return InetAddress(localaddr);
}

coropact::base::Result<InetAddress> get_peer_addr(int fd) {
  sockaddr_in peeraddr{};
  socklen_t addrlen = static_cast<socklen_t>(sizeof(peeraddr));
  if (::getpeername(fd, reinterpret_cast<sockaddr*>(&peeraddr), &addrlen) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }
  if (peeraddr.sin_family != AF_INET) {
    return std::unexpected(
        coropact::base::Error(std::make_error_code(std::errc::address_family_not_supported)));
  }
  return InetAddress(peeraddr);
}

coropact::base::Result<bool> IsSelfConnect(int fd) {
  auto localaddr = get_local_addr(fd);
  if (!localaddr) return std::unexpected(localaddr.error());
  auto peeraddr = get_peer_addr(fd);
  if (!peeraddr) return std::unexpected(peeraddr.error());
  return *localaddr == *peeraddr;
}

}  // namespace coropact::net
