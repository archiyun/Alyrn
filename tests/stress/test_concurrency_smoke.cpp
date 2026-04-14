#include <gtest/gtest.h>

#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/net_utils.h"
#include "runtime/net/tcp_server.h"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

namespace runtime::net {
namespace {

using namespace std::chrono_literals;

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

int ConnectToServer(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(fd, 0);

    sockaddr_in server_addr = MakeIPv4Address("127.0.0.1", port);
    EXPECT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)),
              0);
    return fd;
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

TEST(ConcurrencySmokeTest, EchoServerHandlesParallelClients) {
    const std::uint16_t port = ReserveLoopbackPort();
    std::promise<EventLoop*> ready_promise;
    auto ready_future = ready_promise.get_future();

    std::thread server_thread([&] {
        EventLoop loop;
        TcpServer server(&loop, InetAddress(port, "127.0.0.1"), "ConcurrentEchoServer");
        server.SetThreadNum(2);
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

    constexpr int kClientCount = 8;
    constexpr int kMessagesPerClient = 25;
    std::atomic<int> completed_clients{0};
    std::vector<std::thread> clients;
    clients.reserve(kClientCount);

    for (int client_index = 0; client_index < kClientCount; ++client_index) {
        clients.emplace_back([&, client_index] {
            const int fd = ConnectToServer(port);
            for (int message_index = 0; message_index < kMessagesPerClient; ++message_index) {
                const std::string payload = "client-" + std::to_string(client_index) +
                                            "-msg-" + std::to_string(message_index);
                WriteAll(fd, payload);
                EXPECT_EQ(ReadExactly(fd, payload.size()), payload);
            }
            ++completed_clients;
            ::close(fd);
        });
    }

    for (auto& client : clients) {
        client.join();
    }

    EXPECT_EQ(completed_clients.load(), kClientCount);

    loop->Quit();
    server_thread.join();
}

}  // namespace
}  // namespace runtime::net
