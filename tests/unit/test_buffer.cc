#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <string>

#include "runtime/net/buffer.h"

namespace runtime::net {
namespace {

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
    std::array<int, 2> fds{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()), 0);

    Buffer buffer;
    const std::string inbound = "ping-from-peer";
    ASSERT_EQ(::write(fds[0], inbound.data(), inbound.size()),
              static_cast<ssize_t>(inbound.size()));

    int saved_errno = 0;
    const ssize_t bytes_read = buffer.ReadFd(fds[1], &saved_errno);
    EXPECT_EQ(bytes_read, static_cast<ssize_t>(inbound.size()));
    EXPECT_EQ(saved_errno, 0);
    EXPECT_EQ(buffer.RetrieveAllAsString(), inbound);

    const std::string outbound = "pong-from-buffer";
    buffer.Append(outbound);
    const ssize_t bytes_written = buffer.WriteFd(fds[1], &saved_errno);
    EXPECT_EQ(bytes_written, static_cast<ssize_t>(outbound.size()));
    EXPECT_EQ(saved_errno, 0);
    EXPECT_EQ(buffer.readable_bytes(), 0U);

    std::string received(outbound.size(), '\0');
    ASSERT_EQ(::read(fds[0], received.data(), received.size()),
              static_cast<ssize_t>(received.size()));
    EXPECT_EQ(received, outbound);

    ::close(fds[0]);
    ::close(fds[1]);
}

TEST(BufferTest, ReadFdExpandsStorageForLargePayload) {
    std::array<int, 2> fds{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()), 0);

    const std::string large_payload(8192, 'x');
    ASSERT_EQ(::write(fds[0], large_payload.data(), large_payload.size()),
              static_cast<ssize_t>(large_payload.size()));

    Buffer buffer(32);
    int saved_errno = 0;
    const ssize_t bytes_read = buffer.ReadFd(fds[1], &saved_errno);
    EXPECT_EQ(bytes_read, static_cast<ssize_t>(large_payload.size()));
    EXPECT_EQ(saved_errno, 0);
    EXPECT_EQ(buffer.readable_bytes(), large_payload.size());
    EXPECT_EQ(buffer.RetrieveAllAsString(), large_payload);

    ::close(fds[0]);
    ::close(fds[1]);
}

}  // namespace
}  // namespace runtime::net
