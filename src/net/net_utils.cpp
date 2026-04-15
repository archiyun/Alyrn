#include "runtime/net/net_utils.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace runtime::net {
namespace {

// Reads a file descriptor flag set with fcntl and enables the given bit.
bool SetFdFlag(int fd, int cmd_get, int cmd_set, int flag) {
  const int old_flag = ::fcntl(fd, cmd_get, 0);
  if (old_flag < 0) {
    return false;
  }

  if (::fcntl(fd, cmd_set, old_flag | flag) < 0) {
    return false;
  }

  return true;
}

}  // namespace

int CreateNonBlockingSocket() {
#if defined(__linux__)
  // Linux can apply both flags atomically at socket creation time.
  int sockfd = ::socket(
      AF_INET,
      SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
      IPPROTO_TCP);
#else
  int sockfd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sockfd >= 0) {
    SetNonBlocking(sockfd);
    SetCloseOnExec(sockfd);
  }
#endif

  return sockfd;
}

bool SetNonBlocking(int fd) {
  return SetFdFlag(fd, F_GETFL, F_SETFL, O_NONBLOCK);
}

bool SetCloseOnExec(int fd) {
  return SetFdFlag(fd, F_GETFD, F_SETFD, FD_CLOEXEC);
}

bool SetReuseAddr(int fd, bool on) {
  const int optval = on ? 1 : 0;
  const int ret = ::setsockopt(
      fd,
      SOL_SOCKET,
      SO_REUSEADDR,
      &optval,
      static_cast<socklen_t>(sizeof(optval)));
  return ret == 0;
}

bool SetReusePort(int fd, bool on) {
#ifdef SO_REUSEPORT
  const int optval = on ? 1 : 0;
  const int ret = ::setsockopt(
      fd,
      SOL_SOCKET,
      SO_REUSEPORT,
      &optval,
      static_cast<socklen_t>(sizeof(optval)));
  return ret == 0;
#else
  (void)fd;
  (void)on;
  return false;
#endif
}

bool SetTcpNoDelay(int fd, bool on) {
  const int optval = on ? 1 : 0;
  const int ret = ::setsockopt(
      fd,
      IPPROTO_TCP,
      TCP_NODELAY,
      &optval,
      static_cast<socklen_t>(sizeof(optval)));
  return ret == 0;
}

bool SetKeepAlive(int fd, bool on) {
  const int optval = on ? 1 : 0;
  const int ret = ::setsockopt(
      fd,
      SOL_SOCKET,
      SO_KEEPALIVE,
      &optval,
      static_cast<socklen_t>(sizeof(optval)));
  return ret == 0;
}

void IgnoreSigPipe() {
  // Writing to a closed peer socket would otherwise raise SIGPIPE and may
  // terminate the process. The networking layer handles write errors explicitly.
  ::signal(SIGPIPE, SIG_IGN);
}

sockaddr_in MakeIPv4Address(const std::string& ip, std::uint16_t port) {
  sockaddr_in addr;
  ::bzero(&addr, sizeof(addr));

  addr.sin_family = AF_INET;
  addr.sin_port = ::htons(port);

  // Fall back to loopback if the input string is not a valid IPv4 address.
  if (::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
  }

  return addr;
}

std::string ToIp(const sockaddr_in& addr) {
  char buf[INET_ADDRSTRLEN] = {0};
  const char* ret = ::inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
  return ret == nullptr ? std::string() : std::string(ret);
}

std::string ToPort(const sockaddr_in& addr) {
  return std::to_string(::ntohs(addr.sin_port));
}

std::string ToIpPort(const sockaddr_in& addr) {
  return ToIp(addr) + ":" + ToPort(addr);
}

sockaddr_in GetLocalAddr(int fd) {
  sockaddr_in localaddr;
  ::bzero(&localaddr, sizeof(localaddr));

  socklen_t addrlen = static_cast<socklen_t>(sizeof(localaddr));
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&localaddr), &addrlen) < 0) {
    ::bzero(&localaddr, sizeof(localaddr));
  }

  return localaddr;
}

sockaddr_in GetPeerAddr(int fd) {
  sockaddr_in peeraddr;
  ::bzero(&peeraddr, sizeof(peeraddr));

  socklen_t addrlen = static_cast<socklen_t>(sizeof(peeraddr));
  if (::getpeername(fd, reinterpret_cast<sockaddr*>(&peeraddr), &addrlen) < 0) {
    ::bzero(&peeraddr, sizeof(peeraddr));
  }

  return peeraddr;
}

bool IsSelfConnect(int fd) {
  const sockaddr_in localaddr = GetLocalAddr(fd);
  const sockaddr_in peeraddr = GetPeerAddr(fd);

  return localaddr.sin_port == peeraddr.sin_port &&
         localaddr.sin_addr.s_addr == peeraddr.sin_addr.s_addr;
}

}  // namespace runtime::net
