// SPDX-License-Identifier: MIT
#pragma once

#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <expected>
#include <limits>
#include <system_error>
#include <utility>

#include "coropact/base/check.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/tcp_options.h"
#include "coropact/result.h"
#include "coropact/utils/macros.h"

namespace coropact::net {

namespace detail {

[[nodiscard]]
inline Result<void> SetDescriptorFlag(int fd, int get_command, int set_command, int flag,
                                      bool enabled) noexcept {
  const int old_flags = ::fcntl(fd, get_command, 0);
  if (old_flags < 0) {
    return std::unexpected(CurrentErrno());
  }

  const int new_flags = enabled ? (old_flags | flag) : (old_flags & ~flag);
  if (new_flags != old_flags && ::fcntl(fd, set_command, new_flags) < 0) {
    return std::unexpected(CurrentErrno());
  }
  return {};
}

template <class T>
[[nodiscard]]
inline Result<void> SetSocketOptionValue(int fd, int level, int option, const T& value) noexcept {
  if (fd < 0) {
    return std::unexpected(Errno(EBADF));
  }
  if (::setsockopt(fd, level, option, &value, static_cast<socklen_t>(sizeof(value))) < 0) {
    return std::unexpected(CurrentErrno());
  }
  return {};
}

[[nodiscard]]
inline Result<void> SetSocketOption(int fd, int level, int option, bool enabled) noexcept {
  const int value = enabled ? 1 : 0;
  return SetSocketOptionValue(fd, level, option, value);
}

[[nodiscard]]
inline Result<int> CheckedSocketOptionSize(std::size_t bytes) noexcept {
  if (bytes > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return std::unexpected(Errno(EINVAL));
  }
  return static_cast<int>(bytes);
}

[[nodiscard]]
inline Result<Endpoint> GetEndpoint(int fd, bool peer) noexcept {
  sockaddr_storage address{};
  auto length = static_cast<socklen_t>(sizeof(address));
  const int result = peer ? ::getpeername(fd, reinterpret_cast<sockaddr*>(&address), &length)
                          : ::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length);
  if (result < 0) {
    return std::unexpected(CurrentErrno());
  }
  if (address.ss_family != AF_INET && address.ss_family != AF_INET6) {
    return std::unexpected(Error(std::make_error_code(std::errc::address_family_not_supported)));
  }
  return Endpoint(reinterpret_cast<const sockaddr*>(&address), length);
}

[[nodiscard]]
inline Result<void> ConfigureNonBlockingCloseOnExec(int fd) noexcept {
  auto non_blocking = SetDescriptorFlag(fd, F_GETFL, F_SETFL, O_NONBLOCK, true);
  if (!non_blocking.has_value()) {
    return std::unexpected(non_blocking.error());
  }
  return SetDescriptorFlag(fd, F_GETFD, F_SETFD, FD_CLOEXEC, true);
}

}  // namespace detail

// Creates a non-blocking TCP socket with close-on-exec enabled. Linux keeps
// its atomic creation fast path; other POSIX systems configure the descriptor
// immediately after creation.
[[nodiscard]]
inline Result<int> CreateNonBlockingSocket(sa_family_t family = AF_INET) noexcept {
  if (family != AF_INET && family != AF_INET6) {
    return std::unexpected(Error(std::make_error_code(std::errc::address_family_not_supported)));
  }

  int type = SOCK_STREAM;
#if defined(__linux__)
  type |= SOCK_NONBLOCK | SOCK_CLOEXEC;
#endif
  const int fd = ::socket(family, type, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(CurrentErrno());
  }

#if !defined(__linux__)
  auto configured = detail::ConfigureNonBlockingCloseOnExec(fd);
  if (!configured.has_value()) {
    const Error error = configured.error();
    (void)::close(fd);
    return std::unexpected(error);
  }
#endif
  return fd;
}

[[nodiscard]]
inline Result<void> SetNonBlocking(int fd, bool enabled = true) noexcept {
  return detail::SetDescriptorFlag(fd, F_GETFL, F_SETFL, O_NONBLOCK, enabled);
}

[[nodiscard]]
inline Result<void> SetCloseOnExec(int fd, bool enabled = true) noexcept {
  return detail::SetDescriptorFlag(fd, F_GETFD, F_SETFD, FD_CLOEXEC, enabled);
}

[[nodiscard]]
inline Result<void> SetReuseAddr(int fd, bool enabled = true) noexcept {
  return detail::SetSocketOption(fd, SOL_SOCKET, SO_REUSEADDR, enabled);
}

[[nodiscard]]
inline Result<void> SetReusePort(int fd, bool enabled = true) noexcept {
#if defined(SO_REUSEPORT)
  return detail::SetSocketOption(fd, SOL_SOCKET, SO_REUSEPORT, enabled);
#else
  (void)fd;
  (void)enabled;
  return std::unexpected(Errno(ENOTSUP));
#endif
}

[[nodiscard]]
inline Result<void> SetNoDelay(int fd, bool enabled = true) noexcept {
  return detail::SetSocketOption(fd, IPPROTO_TCP, TCP_NODELAY, enabled);
}

// Compatibility spelling retained for existing callers.
[[nodiscard]]
inline Result<void> SetTcpNoDelay(int fd, bool enabled = true) noexcept {
  return SetNoDelay(fd, enabled);
}

[[nodiscard]]
inline Result<void> SetKeepAlive(int fd, bool enabled = true) noexcept {
  return detail::SetSocketOption(fd, SOL_SOCKET, SO_KEEPALIVE, enabled);
}

[[nodiscard]]
inline Result<void> SetKeepAlivePeriod(int fd, time::Duration period) noexcept {
  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(period).count();
  if (seconds <= 0 || seconds > std::numeric_limits<int>::max()) {
    return std::unexpected(Errno(EINVAL));
  }

  const int value = static_cast<int>(seconds);
#if defined(TCP_KEEPIDLE)
  return detail::SetSocketOptionValue(fd, IPPROTO_TCP, TCP_KEEPIDLE, value);
#elif defined(TCP_KEEPALIVE)
  return detail::SetSocketOptionValue(fd, IPPROTO_TCP, TCP_KEEPALIVE, value);
#else
  (void)fd;
  (void)value;
  return std::unexpected(Errno(ENOTSUP));
#endif
}

[[nodiscard]]
inline Result<void> SetReadBuffer(int fd, std::size_t bytes) noexcept {
  auto value = detail::CheckedSocketOptionSize(bytes);
  if (!value.has_value()) {
    return std::unexpected(value.error());
  }
  return detail::SetSocketOptionValue(fd, SOL_SOCKET, SO_RCVBUF, *value);
}

[[nodiscard]]
inline Result<void> SetWriteBuffer(int fd, std::size_t bytes) noexcept {
  auto value = detail::CheckedSocketOptionSize(bytes);
  if (!value.has_value()) {
    return std::unexpected(value.error());
  }
  return detail::SetSocketOptionValue(fd, SOL_SOCKET, SO_SNDBUF, *value);
}

[[nodiscard]]
inline Result<void> ApplyTcpOptions(int fd, const TcpOptions& options) noexcept {
  if (options.no_delay.has_value()) {
    auto result = SetNoDelay(fd, *options.no_delay);
    if (!result.has_value()) {
      return std::unexpected(result.error());
    }
  }

  if (options.keep_alive.has_value()) {
    auto result = SetKeepAlive(fd, *options.keep_alive);
    if (!result.has_value()) {
      return std::unexpected(result.error());
    }
  }

  if (options.keep_alive_period.has_value()) {
    auto result = SetKeepAlivePeriod(fd, *options.keep_alive_period);
    if (!result.has_value()) {
      return std::unexpected(result.error());
    }
  }

  if (options.read_buffer.has_value()) {
    auto result = SetReadBuffer(fd, *options.read_buffer);
    if (!result.has_value()) {
      return std::unexpected(result.error());
    }
  }

  if (options.write_buffer.has_value()) {
    auto result = SetWriteBuffer(fd, *options.write_buffer);
    if (!result.has_value()) {
      return std::unexpected(result.error());
    }
  }

  return {};
}

[[nodiscard]]
inline Result<Endpoint> GetLocalEndpoint(int fd) noexcept {
  return detail::GetEndpoint(fd, false);
}

[[nodiscard]]
inline Result<Endpoint> GetPeerEndpoint(int fd) noexcept {
  return detail::GetEndpoint(fd, true);
}

[[nodiscard]]
inline Result<bool> IsSelfConnected(int fd) noexcept {
  auto local = GetLocalEndpoint(fd);
  if (!local.has_value()) {
    return std::unexpected(local.error());
  }
  auto peer = GetPeerEndpoint(fd);
  if (!peer.has_value()) {
    return std::unexpected(peer.error());
  }
  return *local == *peer;
}

// Socket is an RAII wrapper around a socket file descriptor.
//
// It owns the descriptor for its lifetime and exposes a small set of socket
// operations used by the networking layer.
class Socket {
public:
  COROPACT_DELETE_COPY(Socket);

  explicit Socket(int sockfd) noexcept : sockfd_(sockfd) {}

  ~Socket() noexcept {
    if (sockfd_ >= 0) {
      const int fd = std::exchange(sockfd_, -1);
      ::close(fd);
    }
  }

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

  [[nodiscard]]
  int fd() const noexcept {
    return sockfd_;
  }

  // Relinquishes ownership without closing. The caller becomes responsible
  // for the descriptor.
  [[nodiscard]]
  int Release() noexcept {
    return std::exchange(sockfd_, -1);
  }

  // Binds the socket to a local address.
  void BindAddress(const Endpoint& localaddr) const noexcept {
    const int ret = ::bind(sockfd_, localaddr.SockAddr(), localaddr.SockAddrLen());
    COROPACT_CHECK(ret == 0, "Socket::BindAddress: bind failed");
  }

  // Marks the socket as a passive listening socket.
  void Listen() const noexcept {
    const int ret = ::listen(sockfd_, SOMAXCONN);
    COROPACT_CHECK(ret == 0, "Socket::Listen: listen failed");
  }

  // Accepts a new inbound connection and configures it as non-blocking and
  // close-on-exec. Linux uses accept4 atomically; other POSIX systems use the
  // portable accept plus fcntl sequence.
  int Accept(Endpoint* peeraddr) const noexcept {
    sockaddr_storage addr{};
    auto len = static_cast<socklen_t>(sizeof(addr));

    int connfd = -1;
#if defined(__linux__)
    connfd =
        ::accept4(sockfd_, reinterpret_cast<sockaddr*>(&addr), &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
    connfd = ::accept(sockfd_, reinterpret_cast<sockaddr*>(&addr), &len);
    if (connfd >= 0) {
      auto configured = detail::ConfigureNonBlockingCloseOnExec(connfd);
      if (!configured.has_value()) {
        const int error = configured.error().value();
        (void)::close(connfd);
        errno = error;
        return -1;
      }
    }
#endif
    if (connfd >= 0 && peeraddr != nullptr) {
      *peeraddr = Endpoint(reinterpret_cast<const sockaddr*>(&addr), len);
    }

    return connfd;
  }

  // Shuts down the write side of the socket. This does not close the
  // descriptor and therefore leaves reads available to the caller.
  [[nodiscard]] Result<void> ShutdownWrite() const noexcept {
    if (sockfd_ < 0) {
      return std::unexpected(Errno(EBADF));
    }
    if (::shutdown(sockfd_, SHUT_WR) < 0) {
      return std::unexpected(CurrentErrno());
    }
    return {};
  }

  // Shuts down the read side of the socket. This does not close the
  // descriptor and therefore leaves writes available to the caller.
  [[nodiscard]] Result<void> ShutdownRead() const noexcept {
    if (sockfd_ < 0) {
      return std::unexpected(Errno(EBADF));
    }
    if (::shutdown(sockfd_, SHUT_RD) < 0) {
      return std::unexpected(CurrentErrno());
    }
    return {};
  }

  // Closes the descriptor before destruction. Idempotent.
  void Close() noexcept {
    if (sockfd_ < 0) {
      return;
    }

    const int fd = std::exchange(sockfd_, -1);
    (void)::close(fd);
  }

  // Enables or disables TCP_NODELAY.
  [[nodiscard]]
  Result<void> SetNoDelay(bool on) const noexcept {
    return ::coropact::net::SetNoDelay(sockfd_, on);
  }

  // Compatibility spelling retained for existing callers.
  [[nodiscard]]
  Result<void> SetTcpNoDelay(bool on) const noexcept {
    return SetNoDelay(on);
  }

  // Enables or disables SO_REUSEADDR.
  void SetReuseAddr(bool on) noexcept {
    const auto result = ::coropact::net::SetReuseAddr(sockfd_, on);
    COROPACT_CHECK(result.has_value(), "Socket::SetReuseAddr: setsockopt failed");
  }

  // Enables or disables SO_REUSEPORT.
  void SetReusePort(bool on) noexcept {
    const auto result = ::coropact::net::SetReusePort(sockfd_, on);
    COROPACT_CHECK(result.has_value(), "Socket::SetReusePort: setsockopt failed");
  }

  // Enables or disables SO_KEEPALIVE.
  [[nodiscard]]
  Result<void> SetKeepAlive(bool on) const noexcept {
    return ::coropact::net::SetKeepAlive(sockfd_, on);
  }

  [[nodiscard]]
  Result<void> SetKeepAlivePeriod(time::Duration period) const noexcept {
    return ::coropact::net::SetKeepAlivePeriod(sockfd_, period);
  }

  [[nodiscard]]
  Result<void> SetReadBuffer(std::size_t bytes) const noexcept {
    return ::coropact::net::SetReadBuffer(sockfd_, bytes);
  }

  [[nodiscard]]
  Result<void> SetWriteBuffer(std::size_t bytes) const noexcept {
    return ::coropact::net::SetWriteBuffer(sockfd_, bytes);
  }

  [[nodiscard]]
  Result<Endpoint> LocalEndpoint() const noexcept {
    return GetLocalEndpoint(sockfd_);
  }

  [[nodiscard]]
  Result<Endpoint> PeerEndpoint() const noexcept {
    return GetPeerEndpoint(sockfd_);
  }

  [[nodiscard]]
  Result<bool> IsSelfConnected() const noexcept {
    return ::coropact::net::IsSelfConnected(sockfd_);
  }

private:
  int sockfd_;
};

}  // namespace coropact::net
