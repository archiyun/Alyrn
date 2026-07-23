// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <string_view>

#include "coropact/base/error.h"
#include "coropact/net/inet_address.h"

#if !defined(__linux__)
#error "coropact::net currently requires Linux"
#endif

namespace coropact::net {

// Parses a numeric IPv4 address. Hostnames are not resolved.
[[nodiscard]] coropact::base::Result<InetAddress> ParseIPv4Address(std::string_view ip,
                                                               std::uint16_t port);

// Creates a non-blocking IPv4 TCP socket with close-on-exec enabled atomically.
[[nodiscard]] coropact::base::Result<int> CreateNonBlockingSocket();

// Sets or clears O_NONBLOCK on the given fd.
[[nodiscard]] coropact::base::Result<void> set_non_blocking(int fd, bool on = true);

// Sets or clears FD_CLOEXEC on the given fd.
[[nodiscard]] coropact::base::Result<void> set_close_on_exec(int fd, bool on = true);

// Enables or disables SO_REUSEADDR on the given socket.
[[nodiscard]] coropact::base::Result<void> set_reuse_addr(int fd, bool on = true);

// Enables or disables SO_REUSEPORT on the given socket.
[[nodiscard]] coropact::base::Result<void> set_reuse_port(int fd, bool on = true);

// Enables or disables TCP_NODELAY on the given socket.
[[nodiscard]] coropact::base::Result<void> set_tcp_non_delay(int fd, bool on = true);

// Enables or disables SO_KEEPALIVE on the given socket.
[[nodiscard]] coropact::base::Result<void> set_keep_alive(int fd, bool on = true);

// Returns the local socket address bound to fd.
[[nodiscard]] coropact::base::Result<InetAddress> get_local_addr(int fd);

// Returns the peer socket address connected to fd.
[[nodiscard]] coropact::base::Result<InetAddress> get_peer_addr(int fd);

// Returns true if fd is connected to itself.
//
// A self-connect usually indicates that the local and peer endpoints refer to
// the same address and port pair.
[[nodiscard]] coropact::base::Result<bool> IsSelfConnect(int fd);

}  // namespace coropact::net
