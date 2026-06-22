// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cstdint>
#include <string>

namespace vexo::net {

// InetAddress is a lightweight wrapper around sockaddr_in.
//
// It is used throughout the networking layer to pass, store, and format IPv4
// socket addresses in a more convenient C++ form.
class InetAddress {
public:
  // Builds a loopback endpoint.
  explicit InetAddress(std::uint16_t port);
  explicit InetAddress(const struct sockaddr_in& addr) : addr_(addr) {}

  std::string ToIp() const;
  std::string ToIpPort() const;
  std::uint16_t ToPort() const;

  const struct sockaddr_in& sock_addr() const { return addr_; }

  friend bool operator==(const InetAddress& lhs, const InetAddress& rhs) noexcept;

private:
  struct sockaddr_in addr_ {};
};

}  // namespace vexo::net
