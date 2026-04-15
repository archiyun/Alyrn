#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/net_utils.h"
#include "runtime/net/tcp_server.h"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>

namespace {

using namespace std::chrono_literals;

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "[FAIL] " << message << '\n';
        return false;
    }
    return true;
}

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
    if (fd < 0) {
        return 0;
    }

    int on = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return 0;
    }

    socklen_t len = static_cast<socklen_t>(sizeof(addr));
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        return 0;
    }

    const std::uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

ScopedFd ConnectToServer(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return ScopedFd();
    }

    sockaddr_in server_addr = runtime::net::MakeIPv4Address("127.0.0.1", port);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        ::close(fd);
        return ScopedFd();
    }
    return ScopedFd(fd);
}

bool WriteAll(int fd, const std::string& payload) {
    std::size_t written = 0;
    while (written < payload.size()) {
        const ssize_t n = ::write(fd, payload.data() + written, payload.size() - written);
        if (n <= 0) {
            return false;
        }
        written += static_cast<std::size_t>(n);
    }
    return true;
}

bool ReadExactly(int fd, std::size_t bytes, std::string* out) {
    std::string result(bytes, '\0');
    std::size_t read_total = 0;
    while (read_total < bytes) {
        const ssize_t n = ::read(fd, result.data() + read_total, bytes - read_total);
        if (n <= 0) {
            return false;
        }
        read_total += static_cast<std::size_t>(n);
    }
    *out = std::move(result);
    return true;
}

bool TestThreadInitCallbackRunsForEachIoLoop() {
    const std::uint16_t port = ReserveLoopbackPort();
    if (!Expect(port != 0, "should reserve an ephemeral loopback port")) {
        return false;
    }

    std::promise<runtime::net::EventLoop*> ready_promise;
    auto ready_future = ready_promise.get_future();

    std::mutex mutex;
    std::condition_variable cv;
    int init_count = 0;

    std::thread server_thread([&] {
        runtime::net::EventLoop loop;
        runtime::net::TcpServer server(
            &loop, runtime::net::InetAddress(port, "127.0.0.1"), "ThreadInitServer");
        server.SetThreadNum(2);
        server.SetThreadInitCallback([&](runtime::net::EventLoop*) {
            std::lock_guard<std::mutex> lock(mutex);
            ++init_count;
            cv.notify_all();
        });
        server.Start();
        ready_promise.set_value(&loop);
        loop.Loop();
    });

    runtime::net::EventLoop* loop = ready_future.get();

    bool ok = true;
    {
        std::unique_lock<std::mutex> lock(mutex);
        ok &= Expect(cv.wait_for(lock, 2s, [&] { return init_count == 2; }),
                     "thread init callback should run once per IO loop");
    }

    loop->Quit();
    server_thread.join();
    return ok;
}

bool TestEchoRoundTripAndLifecycle() {
    const std::uint16_t port = ReserveLoopbackPort();
    if (!Expect(port != 0, "should reserve an ephemeral loopback port")) {
        return false;
    }

    std::promise<runtime::net::EventLoop*> ready_promise;
    auto ready_future = ready_promise.get_future();
    std::atomic<int> connection_events{0};

    std::thread server_thread([&] {
        runtime::net::EventLoop loop;
        runtime::net::TcpServer server(
            &loop, runtime::net::InetAddress(port, "127.0.0.1"), "EchoServer");
        server.SetThreadNum(1);
        server.SetConnectionCallback([&](const runtime::net::TcpServer::TcpConnectionPtr&) {
            connection_events.fetch_add(1, std::memory_order_relaxed);
        });
        server.SetMessageCallback(
            [](const runtime::net::TcpServer::TcpConnectionPtr& conn,
               runtime::net::Buffer& buffer, runtime::time::Timestamp) {
                const std::string message = buffer.RetrieveAllAsString();
                conn->Send(message);
            });
        server.Start();
        ready_promise.set_value(&loop);
        loop.Loop();
    });

    runtime::net::EventLoop* loop = ready_future.get();
    ScopedFd client = ConnectToServer(port);
    bool ok = Expect(client.get() >= 0, "client should connect to the echo server");

    const std::string payload = "hello over tcp";
    ok &= Expect(WriteAll(client.get(), payload), "client should write full payload");

    std::string echoed;
    ok &= Expect(ReadExactly(client.get(), payload.size(), &echoed),
                 "client should read echoed payload");
    ok &= Expect(echoed == payload, "echoed payload should match sent payload");

    client = ScopedFd();

    for (int i = 0; i < 50 && connection_events.load(std::memory_order_relaxed) < 2; ++i) {
        std::this_thread::sleep_for(20ms);
    }
    ok &= Expect(connection_events.load(std::memory_order_relaxed) == 2,
                 "connection callback should fire once on connect and once on close");

    loop->Quit();
    server_thread.join();
    return ok;
}

}  // namespace

int main() {
    try {
        if (!TestThreadInitCallbackRunsForEachIoLoop()) return 1;
        if (!TestEchoRoundTripAndLifecycle()) return 1;
    } catch (const std::exception& ex) {
        std::cerr << "[FAIL] unexpected exception: " << ex.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "[FAIL] unexpected unknown exception\n";
        return 1;
    }

    std::cout << "[PASS] tcp_server_smoke_test\n";
    return 0;
}
