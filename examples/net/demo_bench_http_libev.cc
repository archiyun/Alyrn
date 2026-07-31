// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include <ev.h>

#include "bench_http_common.h"

namespace {

std::atomic_bool g_stop{false};
void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

struct Worker;
struct Connection {
  int fd{-1};
  Worker* worker{nullptr};
  ev_io read_watcher{};
  ev_io write_watcher{};
  std::array<char, coropact_bench::kRequestBufferSize> request{};
  std::size_t used{0};
  std::size_t sent{0};
};

struct Worker {
  struct ev_loop* loop{nullptr};
  ev_io accept_watcher{};
  ev_async stop_watcher{};
  int listener{-1};
  std::uint16_t port{0};
};

void CloseConnection(Connection* connection) {
  ev_io_stop(connection->worker->loop, &connection->read_watcher);
  ev_io_stop(connection->worker->loop, &connection->write_watcher);
  ::close(connection->fd);
  delete connection;
}

void OnWrite(struct ev_loop* loop, ev_io* watcher, int) {
  auto* connection = static_cast<Connection*>(watcher->data);
  const auto& response = coropact_bench::Response();
  while (connection->sent < response.size()) {
    const ssize_t written = ::send(connection->fd, response.data() + connection->sent,
                                   response.size() - connection->sent, MSG_NOSIGNAL);
    if (written > 0) {
      connection->sent += static_cast<std::size_t>(written);
      continue;
    }
    if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
    CloseConnection(connection);
    return;
  }
  connection->used = 0;
  connection->sent = 0;
  ev_io_stop(loop, &connection->write_watcher);
  ev_io_start(loop, &connection->read_watcher);
}

void OnRead(struct ev_loop* loop, ev_io* watcher, int) {
  auto* connection = static_cast<Connection*>(watcher->data);
  for (;;) {
    const ssize_t received = ::recv(connection->fd, connection->request.data() + connection->used,
                                    connection->request.size() - connection->used, 0);
    if (received > 0) {
      connection->used += static_cast<std::size_t>(received);
      if (coropact_bench::HasHeaderTerminator(connection->request.data(), connection->used)) {
        ev_io_stop(loop, &connection->read_watcher);
        ev_io_start(loop, &connection->write_watcher);
        return;
      }
      if (connection->used == connection->request.size()) {
        CloseConnection(connection);
        return;
      }
      continue;
    }
    if (received == 0 || (received < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
      CloseConnection(connection);
    }
    return;
  }
}

void OnAccept(struct ev_loop* loop, ev_io* watcher, int) {
  auto* worker = static_cast<Worker*>(watcher->data);
  for (;;) {
    const int fd = ::accept4(worker->listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) return;
      return;
    }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    auto* connection = new Connection;
    connection->fd = fd;
    connection->worker = worker;
    ev_io_init(&connection->read_watcher, OnRead, fd, EV_READ);
    ev_io_init(&connection->write_watcher, OnWrite, fd, EV_WRITE);
    connection->read_watcher.data = connection;
    connection->write_watcher.data = connection;
    ev_io_start(loop, &connection->read_watcher);
  }
}

void OnStop(struct ev_loop* loop, ev_async*, int) { ev_break(loop, EVBREAK_ALL); }

int CreateListener(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) return -1;
  int one = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0 ||
      ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0) {
    ::close(fd);
    return -1;
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 ||
      ::listen(fd, 4096) < 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

void RunWorker(Worker* worker) {
  worker->loop = ev_loop_new(EVFLAG_AUTO);
  worker->listener = CreateListener(worker->port);
  if (worker->loop == nullptr || worker->listener < 0) return;
  ev_io_init(&worker->accept_watcher, OnAccept, worker->listener, EV_READ);
  worker->accept_watcher.data = worker;
  ev_io_start(worker->loop, &worker->accept_watcher);
  ev_async_init(&worker->stop_watcher, OnStop);
  ev_async_start(worker->loop, &worker->stop_watcher);
  ev_run(worker->loop, 0);
  ev_loop_destroy(worker->loop);
  ::close(worker->listener);
  worker->loop = nullptr;
  worker->listener = -1;
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  const auto port = static_cast<std::uint16_t>(coropact_bench::EnvInt("PORT", 19090));
  const std::size_t workers = coropact_bench::EnvSize("LIBEV_WORKERS", 4);
  if (port == 0 || workers == 0) return 2;
  std::vector<Worker> worker_state(workers);
  std::vector<std::thread> worker_threads;
  worker_threads.reserve(workers);
  for (std::size_t i = 0; i < workers; ++i) {
    worker_state[i].port = port;
    worker_threads.emplace_back(RunWorker, &worker_state[i]);
  }
  std::printf("HttpLibevBench bind=127.0.0.1 port=%u workers=%zu response_body=%zu\n", port,
              workers, coropact_bench::kResponseBodySize);
  std::fflush(stdout);
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  for (auto& worker : worker_state) {
    if (worker.loop != nullptr) ev_async_send(worker.loop, &worker.stop_watcher);
  }
  for (auto& thread : worker_threads) thread.join();
  return 0;
}
