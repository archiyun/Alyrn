#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

#include "alyrn/net/endpoint.h"
#include "alyrn/detail/net/socket.h"

namespace alyrn::net {
namespace {

class ScopedFd {
public:
  explicit ScopedFd(int fd = -1) : fd_(fd) {}
  ~ScopedFd() {
    if (fd_ >= 0) ::close(fd_);
  }

  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;

  ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

  ScopedFd& operator=(ScopedFd&& other) noexcept {
    if (this == &other) return *this;
    if (fd_ >= 0) ::close(fd_);
    fd_ = other.fd_;
    other.fd_ = -1;
    return *this;
  }

  int get() const { return fd_; }

private:
  int fd_;
};

int get_socket_option(int fd, int level, int option) {
  int value = -1;
  socklen_t length = sizeof(value);
  EXPECT_EQ(::getsockopt(fd, level, option, &value, &length), 0);
  return value;
}

TEST(EndpointTest, ParsesAndFormatsNumericIPv4) {
  auto address = ParseIpAddress("127.0.0.1", 8080);

  ASSERT_TRUE(address);
  EXPECT_EQ(address->ToIp(), "127.0.0.1");
  EXPECT_EQ(address->ToPort(), 8080);
  EXPECT_EQ(address->ToIpPort(), "127.0.0.1:8080");
}

TEST(EndpointTest, ParsesAndFormatsNumericIPv6) {
  auto address = ParseIpAddress("::1", 8080);

  ASSERT_TRUE(address);
  EXPECT_EQ(address->family(), Endpoint::Family::kIPv6);
  EXPECT_EQ(address->ToIp(), "::1");
  EXPECT_EQ(address->ToPort(), 8080);
  EXPECT_EQ(address->ToIpPort(), "[::1]:8080");

  const Endpoint loopback = Endpoint::Loopback(8080, Endpoint::Family::kIPv6);
  EXPECT_EQ(loopback, *address);
}

TEST(EndpointTest, RejectsInvalidIpWithoutLoopbackFallback) {
  auto address = ParseIpAddress("not-an-ip", 8080);

  EXPECT_FALSE(address);
  EXPECT_EQ(address.error(), std::make_error_code(std::errc::invalid_argument));

  auto hostname = ParseIpAddress("localhost", 8080);
  EXPECT_FALSE(hostname);
  EXPECT_EQ(hostname.error(), std::make_error_code(std::errc::invalid_argument));
}

TEST(SocketTest, CreatesSocketWithAtomicFlagsAndSupportsClearingThem) {
  auto socket = CreateNonBlockingSocket();
  ASSERT_TRUE(socket);
  ScopedFd fd(*socket);

  EXPECT_NE(::fcntl(fd.get(), F_GETFL, 0) & O_NONBLOCK, 0);
  EXPECT_NE(::fcntl(fd.get(), F_GETFD, 0) & FD_CLOEXEC, 0);

  EXPECT_TRUE(SetNonBlocking(fd.get(), false));
  EXPECT_TRUE(SetCloseOnExec(fd.get(), false));
  EXPECT_EQ(::fcntl(fd.get(), F_GETFL, 0) & O_NONBLOCK, 0);
  EXPECT_EQ(::fcntl(fd.get(), F_GETFD, 0) & FD_CLOEXEC, 0);

  EXPECT_TRUE(SetNonBlocking(fd.get(), true));
  EXPECT_TRUE(SetCloseOnExec(fd.get(), true));
  EXPECT_NE(::fcntl(fd.get(), F_GETFL, 0) & O_NONBLOCK, 0);
  EXPECT_NE(::fcntl(fd.get(), F_GETFD, 0) & FD_CLOEXEC, 0);
}

TEST(SocketTest, ReportsSocketOptionErrorsAndSupportsOnOff) {
  auto socket = CreateNonBlockingSocket();
  ASSERT_TRUE(socket);
  ScopedFd fd(*socket);

  EXPECT_TRUE(SetReuseAddr(fd.get(), true));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_REUSEADDR), 1);
  EXPECT_TRUE(SetReuseAddr(fd.get(), false));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_REUSEADDR), 0);

  EXPECT_TRUE(SetReusePort(fd.get(), true));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_REUSEPORT), 1);
  EXPECT_TRUE(SetReusePort(fd.get(), false));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_REUSEPORT), 0);

  EXPECT_TRUE(SetNoDelay(fd.get(), true));
  EXPECT_EQ(get_socket_option(fd.get(), IPPROTO_TCP, TCP_NODELAY), 1);
  EXPECT_TRUE(SetNoDelay(fd.get(), false));
  EXPECT_EQ(get_socket_option(fd.get(), IPPROTO_TCP, TCP_NODELAY), 0);

  EXPECT_TRUE(SetKeepAlive(fd.get(), true));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_KEEPALIVE), 1);
  EXPECT_TRUE(SetKeepAlive(fd.get(), false));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_KEEPALIVE), 0);

  EXPECT_EQ(SetNonBlocking(-1).error().value(), EBADF);
  EXPECT_EQ(SetCloseOnExec(-1).error().value(), EBADF);
  EXPECT_EQ(SetReuseAddr(-1).error().value(), EBADF);
  EXPECT_EQ(SetNoDelay(-1).error().value(), EBADF);
  EXPECT_EQ(SetReadBuffer(-1, 4096).error().value(), EBADF);
  EXPECT_EQ(SetWriteBuffer(-1, 4096).error().value(), EBADF);
}

TEST(SocketTest, SupportsTcpBufferAndKeepAliveOptions) {
  auto socket = CreateNonBlockingSocket();
  ASSERT_TRUE(socket);
  ScopedFd fd(*socket);

  TcpOptions options;
  options.no_delay = true;
  options.keep_alive = true;
  options.read_buffer = 64 * 1024;
  options.write_buffer = 64 * 1024;

  EXPECT_TRUE(ApplyTcpOptions(fd.get(), options));
  EXPECT_EQ(get_socket_option(fd.get(), IPPROTO_TCP, TCP_NODELAY), 1);
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_KEEPALIVE), 1);
  EXPECT_GT(get_socket_option(fd.get(), SOL_SOCKET, SO_RCVBUF), 0);
  EXPECT_GT(get_socket_option(fd.get(), SOL_SOCKET, SO_SNDBUF), 0);

#if defined(TCP_KEEPIDLE) || defined(TCP_KEEPALIVE)
  EXPECT_TRUE(SetKeepAlivePeriod(fd.get(), time::Seconds(30)));
#endif
  EXPECT_EQ(SetKeepAlivePeriod(fd.get(), time::Duration::zero()).error().value(), EINVAL);
}

TEST(SocketTest, AddressQueriesPreserveErrors) {
  auto local = GetLocalEndpoint(-1);
  EXPECT_FALSE(local);
  EXPECT_EQ(local.error().value(), EBADF);

  auto peer = GetPeerEndpoint(-1);
  EXPECT_FALSE(peer);
  EXPECT_EQ(peer.error().value(), EBADF);

  auto self_connect = IsSelfConnected(-1);
  EXPECT_FALSE(self_connect);
  EXPECT_EQ(self_connect.error().value(), EBADF);
}

TEST(SocketTest, QueriesConnectedIPv4Endpoints) {
  ScopedFd listener(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP));
  ASSERT_GE(listener.get(), 0);

  const Endpoint bind_address(0);
  ASSERT_EQ(::bind(listener.get(), bind_address.SockAddr(), bind_address.SockAddrLen()), 0);
  ASSERT_EQ(::listen(listener.get(), 1), 0);

  auto listening_address = GetLocalEndpoint(listener.get());
  ASSERT_TRUE(listening_address);

  ScopedFd client(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP));
  ASSERT_GE(client.get(), 0);
  ASSERT_EQ(
      ::connect(client.get(), listening_address->SockAddr(), listening_address->SockAddrLen()), 0);

  ScopedFd accepted(::accept4(listener.get(), nullptr, nullptr, SOCK_CLOEXEC));
  ASSERT_GE(accepted.get(), 0);

  auto client_local = GetLocalEndpoint(client.get());
  ASSERT_TRUE(client_local);
  auto client_peer = GetPeerEndpoint(client.get());
  ASSERT_TRUE(client_peer);
  auto server_local = GetLocalEndpoint(accepted.get());
  ASSERT_TRUE(server_local);
  auto server_peer = GetPeerEndpoint(accepted.get());
  ASSERT_TRUE(server_peer);

  EXPECT_EQ(*client_local, *server_peer);
  EXPECT_EQ(*client_peer, *server_local);

  auto self_connect = IsSelfConnected(client.get());
  ASSERT_TRUE(self_connect);
  EXPECT_FALSE(*self_connect);
}

}  // namespace
}  // namespace alyrn::net
