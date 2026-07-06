#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <string>

#include "vexo/net/buffer.h"

namespace vexo::net {
namespace {

struct SocketPair {
  std::array<int, 2> fds{-1, -1};

  ~SocketPair() {
    for (int fd : fds) {
      if (fd >= 0) {
        ::close(fd);
      }
    }
  }
};

TEST(BufferTest, ReusesPrependableSpaceWhenAppendingAfterRetrieve) {
  Buffer buffer(16);
  buffer.Append("hello", 5);
  buffer.Append(" world", 6);

  EXPECT_EQ(buffer.readable_bytes(), 11U);
  EXPECT_EQ(buffer.RetrieveAsString(6), "hello ");
  EXPECT_EQ(buffer.readable_bytes(), 5U);

  const std::string extra = "buffer-growth";
  buffer.Append(extra);

  EXPECT_EQ(buffer.RetrieveAllAsString(), "worldbuffer-growth");
  EXPECT_EQ(buffer.readable_bytes(), 0U);
}

TEST(BufferTest, ReadFdAndWriteFdTransferPayloadThroughSocketPair) {
  SocketPair sockets;
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.fds.data()), 0);

  Buffer buffer;
  const std::string inbound = "ping-from-peer";
  ASSERT_EQ(::write(sockets.fds[0], inbound.data(), inbound.size()),
            static_cast<ssize_t>(inbound.size()));

  int saved_errno = 0;
  const ssize_t bytes_read = buffer.ReadFd(sockets.fds[1], &saved_errno);
  EXPECT_EQ(bytes_read, static_cast<ssize_t>(inbound.size()));
  EXPECT_EQ(saved_errno, 0);
  EXPECT_EQ(buffer.RetrieveAllAsString(), inbound);

  const std::string outbound = "pong-from-buffer";
  buffer.Append(outbound);
  const ssize_t bytes_written = buffer.WriteFd(sockets.fds[1], &saved_errno);
  ASSERT_EQ(bytes_written, static_cast<ssize_t>(outbound.size()));
  ASSERT_EQ(saved_errno, 0);
  ASSERT_EQ(buffer.readable_bytes(), 0U);

  std::string received(outbound.size(), '\0');
  ASSERT_EQ(::read(sockets.fds[0], received.data(), received.size()),
            static_cast<ssize_t>(received.size()));
  EXPECT_EQ(received, outbound);
}

TEST(BufferTest, ReadFdExpandsStorageForLargePayload) {
  SocketPair sockets;
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.fds.data()), 0);

  const std::string large_payload(8192, 'x');
  ASSERT_EQ(::write(sockets.fds[0], large_payload.data(), large_payload.size()),
            static_cast<ssize_t>(large_payload.size()));

  Buffer buffer(32);
  int saved_errno = 0;
  const ssize_t bytes_read = buffer.ReadFd(sockets.fds[1], &saved_errno);
  EXPECT_EQ(bytes_read, static_cast<ssize_t>(large_payload.size()));
  EXPECT_EQ(saved_errno, 0);
  EXPECT_EQ(buffer.readable_bytes(), large_payload.size());
  EXPECT_EQ(buffer.RetrieveAllAsString(), large_payload);
}

}  // namespace
}  // namespace vexo::net
