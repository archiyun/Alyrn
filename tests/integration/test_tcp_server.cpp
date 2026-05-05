#include <gtest/gtest.h>

#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/net_utils.h"
#include "runtime/net/tcp_server.h"

#include <arpa/inet.h>
#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>

namespace runtime::net {
namespace {

using namespace std::chrono_literals;

class ScopedFd {
public:
    explicit ScopedFd(int fd = -1) : fd_(fd) {}
    ~ScopedFd() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;

    ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) {
        other.fd_ = -1;
    }

    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) {
                ::close(fd_);
            }
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    int get() const { return fd_; }

private:
    int fd_;
};

std::uint16_t ReserveLoopbackPort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(fd, 0);

    int on = 1;
    EXPECT_EQ(::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)), 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    EXPECT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);

    socklen_t len = static_cast<socklen_t>(sizeof(addr));
    EXPECT_EQ(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len), 0);

    const std::uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

ScopedFd ConnectToServer(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(fd, 0);

    sockaddr_in server_addr = MakeIPv4Address("127.0.0.1", port);
    const int rc =
        ::connect(fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr));
    EXPECT_EQ(rc, 0);
    return ScopedFd(fd);
}

void WriteAll(int fd, const std::string& payload) {
    std::size_t written = 0;
    while (written < payload.size()) {
        const ssize_t n = ::write(fd, payload.data() + written, payload.size() - written);
        ASSERT_GT(n, 0);
        written += static_cast<std::size_t>(n);
    }
}

std::string ReadExactly(int fd, std::size_t bytes) {
    std::string result(bytes, '\0');
    std::size_t read_total = 0;
    while (read_total < bytes) {
        const ssize_t n = ::read(fd, result.data() + read_total, bytes - read_total);
        if (n <= 0) {
            ADD_FAILURE() << "expected to read " << bytes << " bytes, got " << read_total;
            return {};
        }
        read_total += static_cast<std::size_t>(n);
    }
    return result;
}

TEST(TcpServerTest, ConstructServer) {
    EventLoop loop;
    InetAddress listen_addr(8080, "127.0.0.1");
    TcpServer server(&loop, listen_addr, "TestEchoServer");

    SUCCEED();
}

TEST(TcpServerTest, StartInvokesThreadInitCallbackForEachIoLoop) {
    const std::uint16_t port = ReserveLoopbackPort();
    std::promise<EventLoop*> ready_promise;
    auto ready_future = ready_promise.get_future();

    std::mutex mutex;
    std::condition_variable cv;
    int init_count = 0;

    std::thread server_thread([&] {
        EventLoop loop;
        TcpServer server(&loop, InetAddress(port, "127.0.0.1"), "ThreadInitServer");
        server.SetThreadNum(2);
        server.SetThreadInitCallback([&](EventLoop*) {
            std::lock_guard<std::mutex> lock(mutex);
            ++init_count;
            cv.notify_all();
        });
        server.Start();
        ready_promise.set_value(&loop);
        loop.Loop();
    });

    EventLoop* loop = ready_future.get();
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_TRUE(cv.wait_for(lock, 2s, [&] { return init_count == 2; }));
    }

    loop->Quit();
    server_thread.join();
}

TEST(TcpServerTest, AcceptsConnectionAndEchoesPayloadOnce) {
    const std::uint16_t port = ReserveLoopbackPort();
    std::promise<EventLoop*> ready_promise;
    auto ready_future = ready_promise.get_future();

    std::atomic<int> connection_events{0};

    std::thread server_thread([&] {
        EventLoop loop;
        TcpServer server(&loop, InetAddress(port, "127.0.0.1"), "EchoServer");
        server.SetThreadNum(1);
        server.SetConnectionCallback([&](const TcpServer::TcpConnectionPtr&) {
            connection_events.fetch_add(1, std::memory_order_relaxed);
        });
        server.SetMessageCallback(
            [](const TcpServer::TcpConnectionPtr& conn, Buffer& buffer,
               runtime::time::Timestamp) {
                conn->Send(buffer.RetrieveAllAsString());
            });
        server.Start();
        ready_promise.set_value(&loop);
        loop.Loop();
    });

    EventLoop* loop = ready_future.get();
    ScopedFd client = ConnectToServer(port);
    const std::string payload = "hello over tcp";
    WriteAll(client.get(), payload);
    EXPECT_EQ(ReadExactly(client.get(), payload.size()), payload);

    client = ScopedFd();

    for (int i = 0; i < 50 && connection_events.load(std::memory_order_relaxed) < 2; ++i) {
        std::this_thread::sleep_for(20ms);
    }

    EXPECT_EQ(connection_events.load(std::memory_order_relaxed), 2);

    loop->Quit();
    server_thread.join();
}

}  // namespace
}  // namespace runtime::net
