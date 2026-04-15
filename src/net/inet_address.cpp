#include "runtime/net/inet_address.h"

#include <arpa/inet.h>
#include <cstring>

namespace runtime::net {

InetAddress::InetAddress(std::uint16_t port, std::string ip) {
  std::memset(&addr_, 0, sizeof(addr_));
  addr_.sin_family = AF_INET;
  addr_.sin_port = htons(port);

  if (::inet_pton(AF_INET, ip.c_str(), &addr_.sin_addr) <= 0) {
    // Fall back to loopback if the input string is not a valid IPv4 address.
    addr_.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  }
}

std::string InetAddress::ToIp() const {
  char buffer[INET_ADDRSTRLEN] = {0};
  const char* result =
      ::inet_ntop(AF_INET, &addr_.sin_addr, buffer, sizeof(buffer));
  return result == nullptr ? std::string() : std::string(result);
}

std::string InetAddress::ToIpPort() const {
  return ToIp() + ":" + std::to_string(ToPort());
}

std::uint16_t InetAddress::ToPort() const {
  return ntohs(addr_.sin_port);
}

}  // namespace runtime::net
