#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/net_utils.h"
#include "runtime/net/tcp_server.h"

#include <arpa/inet.h>
#include <atomic>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "[FAIL] " << message << '\n';
        return false;
    }
    return true;
}

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

int ConnectToServer(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    sockaddr_in server_addr = runtime::net::MakeIPv4Address("127.0.0.1", port);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
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

bool TestParallelEchoClients() {
    const std::uint16_t port = ReserveLoopbackPort();
    if (!Expect(port != 0, "should reserve an ephemeral loopback port")) {
        return false;
    }

    std::promise<runtime::net::EventLoop*> ready_promise;
    auto ready_future = ready_promise.get_future();

    std::thread server_thread([&] {
        runtime::net::EventLoop loop;
        runtime::net::TcpServer server(
            &loop,
            runtime::net::InetAddress(port, "127.0.0.1"),
            "ConcurrentEchoServer");
        server.SetThreadNum(2);
        server.SetMessageCallback(
            [](const runtime::net::TcpServer::TcpConnectionPtr& conn,
               runtime::net::Buffer& buffer, runtime::time::Timestamp) {
                conn->Send(buffer.RetrieveAllAsString());
            });
        server.Start();
        ready_promise.set_value(&loop);
        loop.Loop();
    });

    runtime::net::EventLoop* loop = ready_future.get();

    constexpr int kClientCount = 8;
    constexpr int kMessagesPerClient = 25;
    std::atomic<int> completed_clients{0};
    std::atomic<int> failed_clients{0};
    std::vector<std::thread> clients;
    clients.reserve(kClientCount);

    for (int client_index = 0; client_index < kClientCount; ++client_index) {
        clients.emplace_back([&, client_index] {
            const int fd = ConnectToServer(port);
            if (fd < 0) {
                ++failed_clients;
                return;
            }

            bool ok = true;
            for (int message_index = 0; message_index < kMessagesPerClient; ++message_index) {
                const std::string payload = "client-" + std::to_string(client_index) +
                                            "-msg-" + std::to_string(message_index);
                std::string echoed;
                ok &= WriteAll(fd, payload);
                ok &= ReadExactly(fd, payload.size(), &echoed);
                ok &= echoed == payload;
                if (!ok) {
                    break;
                }
            }

            if (ok) {
                ++completed_clients;
            } else {
                ++failed_clients;
            }
            ::close(fd);
        });
    }

    for (auto& client : clients) {
        client.join();
    }

    const bool ok = Expect(failed_clients.load() == 0,
                           "parallel echo clients should not fail") &&
                    Expect(completed_clients.load() == kClientCount,
                           "all parallel clients should finish successfully");

    loop->Quit();
    server_thread.join();
    return ok;
}

}  // namespace

int main() {
    try {
        if (!TestParallelEchoClients()) return 1;
    } catch (const std::exception& ex) {
        std::cerr << "[FAIL] unexpected exception: " << ex.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "[FAIL] unexpected unknown exception\n";
        return 1;
    }

    std::cout << "[PASS] concurrency_smoke_test\n";
    return 0;
}
