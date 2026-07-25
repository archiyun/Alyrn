// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cstdint>
#include <cstring>
#include <string>

#include "coropact/base/check.h"

namespace coropact::net {

// Endpoint is the address value shared by the networking backends.
//
// It deliberately owns the complete sockaddr representation instead of
// exposing an IPv4-specific sockaddr_in. This keeps bind/connect/accept code
// independent of the address family while retaining the address length needed
// by the socket APIs.
class Endpoint {
public:
  enum class Family : std::uint8_t {
    kIPv4 = AF_INET,
    kIPv6 = AF_INET6,
  };

  // Builds a loopback endpoint. IPv4 remains the default for compatibility
  // with existing callers that only provide a port.
  explicit Endpoint(std::uint16_t port, Family family = Family::kIPv4) noexcept
      : Endpoint(Uninitialized{}) {
    Initialize(port, family, true);
  }

  explicit Endpoint(const sockaddr_in& address) noexcept { Assign(address); }

  explicit Endpoint(const sockaddr_in6& address) noexcept { Assign(address); }

  [[nodiscard]]
  static Endpoint Loopback(std::uint16_t port, Family family = Family::kIPv4) noexcept {
    return Endpoint{port, family};
  }

  [[nodiscard]]
  static Endpoint Any(std::uint16_t port, Family family = Family::kIPv6) noexcept {
    Endpoint endpoint(Uninitialized{});
    endpoint.Initialize(port, family, false);
    return endpoint;
  }

  // Builds an endpoint from an address returned by accept4/getsockname or
  // getpeername. The input may be larger than the family-specific structure.
  explicit Endpoint(const sockaddr* address, socklen_t length) noexcept {
    COROPACT_CHECK(address != nullptr, "Endpoint: address must not be null");

    switch (address->sa_family) {
      case AF_INET:
        COROPACT_CHECK(length >= static_cast<socklen_t>(sizeof(sockaddr_in)),
                       "Endpoint: truncated IPv4 address");
        std::memcpy(&addr_, address, sizeof(sockaddr_in));
        addr_len_ = static_cast<socklen_t>(sizeof(sockaddr_in));
        return;
      case AF_INET6:
        COROPACT_CHECK(length >= static_cast<socklen_t>(sizeof(sockaddr_in6)),
                       "Endpoint: truncated IPv6 address");
        std::memcpy(&addr_, address, sizeof(sockaddr_in6));
        addr_len_ = static_cast<socklen_t>(sizeof(sockaddr_in6));
        return;
      default:
        COROPACT_CHECK(false, "Endpoint: unsupported address family");
    }
  }

  [[nodiscard]]
  Family family() const noexcept {
    return static_cast<Family>(addr_.ss_family);
  }

  [[nodiscard]]
  sa_family_t native_family() const noexcept {
    return addr_.ss_family;
  }

  [[nodiscard]]
  std::string ToIp() const {
    char buffer[INET6_ADDRSTRLEN] = {0};
    const void* address = nullptr;
    const char* result = nullptr;

    switch (native_family()) {
      case AF_INET:
        address = &reinterpret_cast<const sockaddr_in&>(addr_).sin_addr;
        result = ::inet_ntop(AF_INET, address, buffer, sizeof(buffer));
        break;
      case AF_INET6:
        address = &reinterpret_cast<const sockaddr_in6&>(addr_).sin6_addr;
        result = ::inet_ntop(AF_INET6, address, buffer, sizeof(buffer));
        break;
      default:
        COROPACT_CHECK(false, "Endpoint::ToIp: unsupported address family");
    }

    COROPACT_CHECK(result != nullptr, "Endpoint::ToIp: inet_ntop failed");
    return result;
  }

  [[nodiscard]]
  std::string ToIpPort() const {
    if (native_family() == AF_INET6) {
      return "[" + ToIp() + "]:" + std::to_string(ToPort());
    }
    return ToIp() + ":" + std::to_string(ToPort());
  }

  [[nodiscard]]
  std::uint16_t ToPort() const noexcept {
    switch (native_family()) {
      case AF_INET:
        return ntohs(reinterpret_cast<const sockaddr_in&>(addr_).sin_port);
      case AF_INET6:
        return ntohs(reinterpret_cast<const sockaddr_in6&>(addr_).sin6_port);
      default:
        return 0;
    }
  }

  [[nodiscard]]
  const sockaddr* sock_addr() const noexcept {
    return reinterpret_cast<const sockaddr*>(&addr_);
  }

  [[nodiscard]]
  socklen_t sock_addr_len() const noexcept {
    return addr_len_;
  }

  friend bool operator==(const Endpoint& lhs, const Endpoint& rhs) noexcept {
    if (lhs.family() != rhs.family() || lhs.ToPort() != rhs.ToPort()) {
      return false;
    }

    switch (lhs.native_family()) {
      case AF_INET:
        return reinterpret_cast<const sockaddr_in&>(lhs.addr_).sin_addr.s_addr ==
               reinterpret_cast<const sockaddr_in&>(rhs.addr_).sin_addr.s_addr;
      case AF_INET6: {
        const auto& lhs_address = reinterpret_cast<const sockaddr_in6&>(lhs.addr_);
        const auto& rhs_address = reinterpret_cast<const sockaddr_in6&>(rhs.addr_);
        return lhs_address.sin6_scope_id == rhs_address.sin6_scope_id &&
               std::memcmp(&lhs_address.sin6_addr, &rhs_address.sin6_addr,
                           sizeof(lhs_address.sin6_addr)) == 0;
      }
      default:
        return false;
    }
  }

  friend bool operator!=(const Endpoint& lhs, const Endpoint& rhs) noexcept {
    return !(lhs == rhs);
  }

private:
  struct Uninitialized {};

  explicit Endpoint(Uninitialized) noexcept {}

  void Initialize(std::uint16_t port, Family family, bool loopback) noexcept {
    switch (family) {
      case Family::kIPv4: {
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        address.sin_addr.s_addr = htonl(loopback ? INADDR_LOOPBACK : INADDR_ANY);
        Assign(address);
        return;
      }
      case Family::kIPv6: {
        sockaddr_in6 address{};
        address.sin6_family = AF_INET6;
        address.sin6_port = htons(port);
        address.sin6_addr = loopback ? in6addr_loopback : in6addr_any;
        Assign(address);
        return;
      }
    }

    COROPACT_CHECK(false, "Endpoint: unsupported address family");
  }

  void Assign(const sockaddr_in& address) noexcept {
    std::memset(&addr_, 0, sizeof(addr_));
    std::memcpy(&addr_, &address, sizeof(address));
    addr_len_ = static_cast<socklen_t>(sizeof(address));
  }

  void Assign(const sockaddr_in6& address) noexcept {
    std::memset(&addr_, 0, sizeof(addr_));
    std::memcpy(&addr_, &address, sizeof(address));
    addr_len_ = static_cast<socklen_t>(sizeof(address));
  }

  sockaddr_storage addr_{};
  socklen_t addr_len_{0};
};

}  // namespace coropact::net
