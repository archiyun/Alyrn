// SPDX-License-Identifier: MIT

#define ASIO_STANDALONE

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "bench_http_common.h"

namespace {

using asio::ip::tcp;
std::atomic_bool g_stop{false};
void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

struct Worker;
struct Connection : std::enable_shared_from_this<Connection> {
  explicit Connection(std::shared_ptr<tcp::socket> socket) : socket(std::move(socket)) {}

  std::shared_ptr<tcp::socket> socket;
  std::array<char, coropact_bench::kRequestBufferSize> request{};
  std::size_t used{0};
};

void ReadRequest(const std::shared_ptr<Connection>& connection);

void WriteResponse(const std::shared_ptr<Connection>& connection) {
  const auto& response = coropact_bench::Response();
  asio::async_write(
      *connection->socket, asio::buffer(response),
      [connection](const asio::error_code& error, std::size_t) {
        if (error) return;
        connection->used = 0;
        ReadRequest(connection);
      });
}

void ReadRequest(const std::shared_ptr<Connection>& connection) {
  if (connection->used == connection->request.size()) return;
  connection->socket->async_read_some(
      asio::buffer(connection->request.data() + connection->used,
                   connection->request.size() - connection->used),
      [connection](const asio::error_code& error, std::size_t received) {
        if (error || received == 0) return;
        connection->used += received;
        if (!coropact_bench::HasHeaderTerminator(connection->request.data(), connection->used)) {
          ReadRequest(connection);
          return;
        }
        WriteResponse(connection);
      });
}

struct Worker {
  Worker(std::size_t index, std::uint16_t port) : index(index), port(port), acceptor(io) {}

  void Run() {
    try {
      const tcp::endpoint endpoint(asio::ip::make_address("127.0.0.1"), port);
      acceptor.open(endpoint.protocol());
      acceptor.set_option(asio::socket_base::reuse_address(true));
      int one = 1;
      ::setsockopt(acceptor.native_handle(), SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
      acceptor.bind(endpoint);
      acceptor.listen(4096);
      StartAccept();
      io.run();
    } catch (const std::exception& error) {
      std::fprintf(stderr, "asio worker %zu failed: %s\n", index, error.what());
    }
  }

  void StartAccept() {
    auto socket = std::make_shared<tcp::socket>(io);
    acceptor.async_accept(
        *socket, [this, socket](const asio::error_code& error) {
          if (!error) {
            int one = 1;
            ::setsockopt(socket->native_handle(), IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            ReadRequest(std::make_shared<Connection>(socket));
          }
          if (acceptor.is_open()) StartAccept();
        });
  }

  void Stop() noexcept { io.stop(); }

  std::size_t index;
  std::uint16_t port;
  asio::io_context io;
  tcp::acceptor acceptor;
};

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  const auto port = static_cast<std::uint16_t>(coropact_bench::EnvInt("PORT", 19090));
  const std::size_t workers = coropact_bench::EnvSize("ASIO_WORKERS", 4);
  if (port == 0 || workers == 0) return 2;

  std::vector<std::unique_ptr<Worker>> worker_state;
  std::vector<std::thread> worker_threads;
  worker_state.reserve(workers);
  worker_threads.reserve(workers);
  for (std::size_t i = 0; i < workers; ++i) {
    worker_state.push_back(std::make_unique<Worker>(i, port));
    worker_threads.emplace_back([worker = worker_state.back().get()] { worker->Run(); });
  }
  std::printf("HttpAsioBench bind=127.0.0.1 port=%u workers=%zu response_body=%zu\n", port,
              workers, coropact_bench::kResponseBodySize);
  std::fflush(stdout);
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  for (auto& worker : worker_state) worker->Stop();
  for (auto& thread : worker_threads) thread.join();
  return 0;
}
