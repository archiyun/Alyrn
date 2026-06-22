#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <exception>
#include <iostream>
#include <string>

#include "vexo/net/buffer.h"

namespace {

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "[FAIL] " << message << '\n';
        return false;
    }
    return true;
}

bool TestReusePrependableSpace() {
    vexo::net::Buffer buffer(16);
    buffer.Append("hello", 5);
    buffer.Append(" world", 6);

    if (!Expect(buffer.RetrieveAsString(6) == "hello ",
                "RetrieveAsString should consume the prefix")) {
        return false;
    }

    buffer.Append("buffer-growth");
    if (!Expect(buffer.RetrieveAllAsString() == "worldbuffer-growth",
                "append after retrieve should preserve unread bytes")) {
        return false;
    }
    return true;
}

bool TestReadFdAndWriteFd() {
    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair should be created")) {
        return false;
    }

    vexo::net::Buffer buffer;
    const std::string inbound = "ping-from-peer";
    if (!Expect(::write(fds[0], inbound.data(), inbound.size()) ==
                    static_cast<ssize_t>(inbound.size()),
                "socketpair write should succeed")) {
        ::close(fds[0]);
        ::close(fds[1]);
        return false;
    }

    int saved_errno = 0;
    if (!Expect(buffer.ReadFd(fds[1], &saved_errno) ==
                    static_cast<ssize_t>(inbound.size()),
                "ReadFd should read the full inbound payload")) {
        ::close(fds[0]);
        ::close(fds[1]);
        return false;
    }
    if (!Expect(saved_errno == 0, "ReadFd should not set errno on success")) {
        ::close(fds[0]);
        ::close(fds[1]);
        return false;
    }
    if (!Expect(buffer.RetrieveAllAsString() == inbound,
                "buffer should expose inbound payload")) {
        ::close(fds[0]);
        ::close(fds[1]);
        return false;
    }

    const std::string outbound = "pong-from-buffer";
    buffer.Append(outbound);
    if (!Expect(buffer.WriteFd(fds[1], &saved_errno) ==
                    static_cast<ssize_t>(outbound.size()),
                "WriteFd should flush the full outbound payload")) {
        ::close(fds[0]);
        ::close(fds[1]);
        return false;
    }

    std::string received(outbound.size(), '\0');
    if (!Expect(::read(fds[0], received.data(), received.size()) ==
                    static_cast<ssize_t>(received.size()),
                "peer should receive flushed bytes")) {
        ::close(fds[0]);
        ::close(fds[1]);
        return false;
    }
    if (!Expect(received == outbound, "peer should receive the exact payload")) {
        ::close(fds[0]);
        ::close(fds[1]);
        return false;
    }

    ::close(fds[0]);
    ::close(fds[1]);
    return true;
}

bool TestLargeReadGrowth() {
    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair should be created")) {
        return false;
    }

    const std::string large_payload(8192, 'x');
    if (!Expect(::write(fds[0], large_payload.data(), large_payload.size()) ==
                    static_cast<ssize_t>(large_payload.size()),
                "large payload write should succeed")) {
        ::close(fds[0]);
        ::close(fds[1]);
        return false;
    }

    vexo::net::Buffer buffer(32);
    int saved_errno = 0;
    if (!Expect(buffer.ReadFd(fds[1], &saved_errno) ==
                    static_cast<ssize_t>(large_payload.size()),
                "ReadFd should expand buffer for large payload")) {
        ::close(fds[0]);
        ::close(fds[1]);
        return false;
    }
    if (!Expect(buffer.RetrieveAllAsString() == large_payload,
                "large payload should round-trip through the buffer")) {
        ::close(fds[0]);
        ::close(fds[1]);
        return false;
    }

    ::close(fds[0]);
    ::close(fds[1]);
    return true;
}

}  // namespace

int main() {
    try {
        if (!TestReusePrependableSpace()) return 1;
        if (!TestReadFdAndWriteFd()) return 1;
        if (!TestLargeReadGrowth()) return 1;
    } catch (const std::exception& ex) {
        std::cerr << "[FAIL] unexpected exception: " << ex.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "[FAIL] unexpected unknown exception\n";
        return 1;
    }

    std::cout << "[PASS] buffer_smoke_test\n";
    return 0;
}
