#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

#include "vexo/net/buffer.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/net_utils.h"

namespace vexo::net {
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

TEST(InetAddressTest, ParsesAndFormatsNumericIPv4) {
  auto address = ParseIPv4Address("127.0.0.1", 8080);

  ASSERT_TRUE(address);
  EXPECT_EQ(address->ToIp(), "127.0.0.1");
  EXPECT_EQ(address->ToPort(), 8080);
  EXPECT_EQ(address->ToIpPort(), "127.0.0.1:8080");
}

TEST(InetAddressTest, RejectsInvalidIPv4WithoutLoopbackFallback) {
  auto address = ParseIPv4Address("not-an-ip", 8080);

  EXPECT_FALSE(address);
  EXPECT_EQ(address.error(), std::make_error_code(std::errc::invalid_argument));

  auto hostname = ParseIPv4Address("localhost", 8080);
  EXPECT_FALSE(hostname);
  EXPECT_EQ(hostname.error(), std::make_error_code(std::errc::invalid_argument));
}

TEST(NetUtilsTest, CreatesSocketWithAtomicFlagsAndSupportsClearingThem) {
  auto socket = CreateNonBlockingSocket();
  ASSERT_TRUE(socket);
  ScopedFd fd(*socket);

  EXPECT_NE(::fcntl(fd.get(), F_GETFL, 0) & O_NONBLOCK, 0);
  EXPECT_NE(::fcntl(fd.get(), F_GETFD, 0) & FD_CLOEXEC, 0);

  EXPECT_TRUE(set_non_blocking(fd.get(), false));
  EXPECT_TRUE(set_close_on_exec(fd.get(), false));
  EXPECT_EQ(::fcntl(fd.get(), F_GETFL, 0) & O_NONBLOCK, 0);
  EXPECT_EQ(::fcntl(fd.get(), F_GETFD, 0) & FD_CLOEXEC, 0);

  EXPECT_TRUE(set_non_blocking(fd.get(), true));
  EXPECT_TRUE(set_close_on_exec(fd.get(), true));
  EXPECT_NE(::fcntl(fd.get(), F_GETFL, 0) & O_NONBLOCK, 0);
  EXPECT_NE(::fcntl(fd.get(), F_GETFD, 0) & FD_CLOEXEC, 0);
}

TEST(NetUtilsTest, ReportsSocketOptionErrorsAndSupportsOnOff) {
  auto socket = CreateNonBlockingSocket();
  ASSERT_TRUE(socket);
  ScopedFd fd(*socket);

  EXPECT_TRUE(set_reuse_addr(fd.get(), true));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_REUSEADDR), 1);
  EXPECT_TRUE(set_reuse_addr(fd.get(), false));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_REUSEADDR), 0);

  EXPECT_TRUE(set_reuse_port(fd.get(), true));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_REUSEPORT), 1);
  EXPECT_TRUE(set_reuse_port(fd.get(), false));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_REUSEPORT), 0);

  EXPECT_TRUE(set_tcp_non_delay(fd.get(), true));
  EXPECT_EQ(get_socket_option(fd.get(), IPPROTO_TCP, TCP_NODELAY), 1);
  EXPECT_TRUE(set_tcp_non_delay(fd.get(), false));
  EXPECT_EQ(get_socket_option(fd.get(), IPPROTO_TCP, TCP_NODELAY), 0);

  EXPECT_TRUE(set_keep_alive(fd.get(), true));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_KEEPALIVE), 1);
  EXPECT_TRUE(set_keep_alive(fd.get(), false));
  EXPECT_EQ(get_socket_option(fd.get(), SOL_SOCKET, SO_KEEPALIVE), 0);

  EXPECT_EQ(set_non_blocking(-1).error().value(), EBADF);
  EXPECT_EQ(set_close_on_exec(-1).error().value(), EBADF);
  EXPECT_EQ(set_reuse_addr(-1).error().value(), EBADF);
}

TEST(NetUtilsTest, AddressQueriesPreserveErrors) {
  auto local = get_local_addr(-1);
  EXPECT_FALSE(local);
  EXPECT_EQ(local.error().value(), EBADF);

  auto peer = get_peer_addr(-1);
  EXPECT_FALSE(peer);
  EXPECT_EQ(peer.error().value(), EBADF);

  auto self_connect = IsSelfConnect(-1);
  EXPECT_FALSE(self_connect);
  EXPECT_EQ(self_connect.error().value(), EBADF);
}

TEST(NetUtilsTest, QueriesConnectedIPv4Endpoints) {
  ScopedFd listener(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP));
  ASSERT_GE(listener.get(), 0);

  const InetAddress bind_address(0);
  ASSERT_EQ(::bind(listener.get(), reinterpret_cast<const sockaddr*>(&bind_address.sock_addr()),
                   sizeof(bind_address.sock_addr())),
            0);
  ASSERT_EQ(::listen(listener.get(), 1), 0);

  auto listening_address = get_local_addr(listener.get());
  ASSERT_TRUE(listening_address);

  ScopedFd client(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP));
  ASSERT_GE(client.get(), 0);
  ASSERT_EQ(
      ::connect(client.get(), reinterpret_cast<const sockaddr*>(&listening_address->sock_addr()),
                sizeof(listening_address->sock_addr())),
      0);

  ScopedFd accepted(::accept4(listener.get(), nullptr, nullptr, SOCK_CLOEXEC));
  ASSERT_GE(accepted.get(), 0);

  auto client_local = get_local_addr(client.get());
  ASSERT_TRUE(client_local);
  auto client_peer = get_peer_addr(client.get());
  ASSERT_TRUE(client_peer);
  auto server_local = get_local_addr(accepted.get());
  ASSERT_TRUE(server_local);
  auto server_peer = get_peer_addr(accepted.get());
  ASSERT_TRUE(server_peer);

  EXPECT_EQ(*client_local, *server_peer);
  EXPECT_EQ(*client_peer, *server_local);

  auto self_connect = IsSelfConnect(client.get());
  ASSERT_TRUE(self_connect);
  EXPECT_FALSE(*self_connect);
}

TEST(NetUtilsTest, SocketWriteDoesNotRaiseSigPipe) {
  int raw_fds[2];
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, raw_fds), 0);
  ScopedFd writer(raw_fds[0]);
  ScopedFd peer(raw_fds[1]);

  peer = ScopedFd();

  Buffer buffer;
  buffer.Append("payload");
  int saved_errno = 0;
  EXPECT_EQ(buffer.WriteFd(writer.get(), &saved_errno), -1);
  EXPECT_EQ(saved_errno, EPIPE);
}

}  // namespace
}  // namespace vexo::net
