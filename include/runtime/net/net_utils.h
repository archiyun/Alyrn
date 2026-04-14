#pragma once

#include <cstdint>
#include <string>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace runtime::net {

// Creates a non-blocking socket with close-on-exec enabled when supported.
//
// Returns the socket fd on success, or a negative value on failure.
int CreateNonBlockingSocket();

// Sets or clears O_NONBLOCK on the given fd.
bool SetNonBlocking(int fd);

// Sets or clears FD_CLOEXEC on the given fd.
bool SetCloseOnExec(int fd);

// Enables or disables SO_REUSEADDR on the given socket.
bool SetReuseAddr(int fd, bool on = true);

// Enables or disables SO_REUSEPORT on the given socket.
bool SetReusePort(int fd, bool on = true);

// Enables or disables TCP_NODELAY on the given socket.
bool SetTcpNonDelay(int fd, bool on = true);

// Enables or disables SO_KEEPALIVE on the given socket.
bool SetKeepAlive(int fd, bool on = true);

// Installs process-wide handling to ignore SIGPIPE.
//
// This prevents the process from being terminated when writing to a socket
// whose peer has already closed the connection.
void IgnoreSigPipe();

// Builds an IPv4 socket address from an IP string and port.
sockaddr_in MakeIPv4Address(const std::string& ip, std::uint16_t port);

// Converts an IPv4 socket address to its textual IP representation.
std::string ToIp(const sockaddr_in& addr);

// Converts an IPv4 socket address to "ip:port" form.
std::string ToIpPort(const sockaddr_in& addr);

// Returns the port portion of an IPv4 socket address as a string.
std::string ToPort(const sockaddr_in& addr);

// Returns the local socket address bound to fd.
sockaddr_in GetLocalAddr(int fd);

// Returns the peer socket address connected to fd.
sockaddr_in GetPeerAddr(int fd);

// Returns true if fd is connected to itself.
//
// A self-connect usually indicates that the local and peer endpoints refer to
// the same address and port pair.
bool IsSelfConnect(int fd);

}  // namespace runtime::net
