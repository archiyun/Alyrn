// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/net/inet_address.h"

#include <arpa/inet.h>

#include <cstdlib>

namespace runtime::net {

InetAddress::InetAddress(std::uint16_t port) {
  addr_.sin_family = AF_INET;
  addr_.sin_port = htons(port);
  addr_.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
}

std::string InetAddress::ToIp() const {
  char buffer[INET_ADDRSTRLEN] = {0};
  const char* result = ::inet_ntop(AF_INET, &addr_.sin_addr, buffer, sizeof(buffer));
  if (result == nullptr) std::abort();
  return result;
}

std::string InetAddress::ToIpPort() const { return ToIp() + ":" + std::to_string(ToPort()); }

std::uint16_t InetAddress::ToPort() const { return ntohs(addr_.sin_port); }

bool operator==(const InetAddress& lhs, const InetAddress& rhs) noexcept {
  return lhs.addr_.sin_family == rhs.addr_.sin_family && lhs.addr_.sin_port == rhs.addr_.sin_port &&
         lhs.addr_.sin_addr.s_addr == rhs.addr_.sin_addr.s_addr;
}

}  // namespace runtime::net
