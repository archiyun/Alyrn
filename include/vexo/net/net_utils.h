// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <optional>
#include <string_view>
#include <system_error>

#include "vexo/net/inet_address.h"

#if !defined(__linux__)
#error "vexo::net currently requires Linux"
#endif

namespace vexo::net {

template <typename T>
struct [[nodiscard]] NetResult {
  std::optional<T> value;
  std::error_code error;

  explicit operator bool() const noexcept { return value.has_value(); }
};

// Parses a numeric IPv4 address. Hostnames are not resolved.
[[nodiscard]] NetResult<InetAddress> ParseIPv4Address(std::string_view ip, std::uint16_t port);

// Creates a non-blocking IPv4 TCP socket with close-on-exec enabled atomically.
[[nodiscard]] NetResult<int> CreateNonBlockingSocket();

// Sets or clears O_NONBLOCK on the given fd.
[[nodiscard]] std::error_code set_non_blocking(int fd, bool on = true);

// Sets or clears FD_CLOEXEC on the given fd.
[[nodiscard]] std::error_code set_close_on_exec(int fd, bool on = true);

// Enables or disables SO_REUSEADDR on the given socket.
[[nodiscard]] std::error_code set_reuse_addr(int fd, bool on = true);

// Enables or disables SO_REUSEPORT on the given socket.
[[nodiscard]] std::error_code set_reuse_port(int fd, bool on = true);

// Enables or disables TCP_NODELAY on the given socket.
[[nodiscard]] std::error_code set_tcp_non_delay(int fd, bool on = true);

// Enables or disables SO_KEEPALIVE on the given socket.
[[nodiscard]] std::error_code set_keep_alive(int fd, bool on = true);

// Returns the local socket address bound to fd.
[[nodiscard]] NetResult<InetAddress> get_local_addr(int fd);

// Returns the peer socket address connected to fd.
[[nodiscard]] NetResult<InetAddress> get_peer_addr(int fd);

// Returns true if fd is connected to itself.
//
// A self-connect usually indicates that the local and peer endpoints refer to
// the same address and port pair.
[[nodiscard]] NetResult<bool> IsSelfConnect(int fd);

}  // namespace vexo::net
