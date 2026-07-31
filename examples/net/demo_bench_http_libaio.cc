// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <errno.h>
#include <libaio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

#include "bench_http_common.h"

namespace {

constexpr unsigned kAioEntries = 8192;
constexpr int kMaxEvents = 256;
std::atomic_bool g_stop{false};

void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

struct Worker;
struct Connection;

struct PollOperation {
  iocb request{};
  Worker* worker{nullptr};
  Connection* connection{nullptr};
  int fd{-1};
  short events{0};
};

struct Connection {
  int fd{-1};
  Worker* worker{nullptr};
  PollOperation poll{};
  std::array<char, coropact_bench::kRequestBufferSize> request{};
  std::size_t used{0};
  std::size_t sent{0};
  bool writing{false};
};

struct Worker {
  io_context_t context{nullptr};
  int listener{-1};
  std::uint16_t port{0};
  PollOperation accept_poll{};
  std::unordered_set<Connection*> connections;
};

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

bool SubmitPoll(Worker* worker, PollOperation* operation, int fd, short events) {
  operation->fd = fd;
  operation->events = events;
  std::memset(&operation->request, 0, sizeof(operation->request));
  io_prep_poll(&operation->request, fd, events);
  operation->request.data = operation;
  iocb* requests[] = {&operation->request};
  return ::io_submit(worker->context, 1, requests) == 1;
}

void CloseConnection(Worker* worker, Connection* connection) {
  worker->connections.erase(connection);
  ::close(connection->fd);
  delete connection;
}

bool StartRead(Worker* worker, Connection* connection) {
  return SubmitPoll(worker, &connection->poll, connection->fd, POLLIN);
}

bool StartWrite(Worker* worker, Connection* connection) {
  return SubmitPoll(worker, &connection->poll, connection->fd, POLLOUT);
}

void HandleRead(Worker* worker, Connection* connection) {
  for (;;) {
    const ssize_t received = ::recv(connection->fd, connection->request.data() + connection->used,
                                    connection->request.size() - connection->used, 0);
    if (received > 0) {
      connection->used += static_cast<std::size_t>(received);
      if (coropact_bench::HasHeaderTerminator(connection->request.data(), connection->used)) {
        connection->writing = true;
        connection->sent = 0;
        if (!StartWrite(worker, connection)) CloseConnection(worker, connection);
        return;
      }
      if (connection->used == connection->request.size()) {
        CloseConnection(worker, connection);
        return;
      }
      continue;
    }
    if (received == 0 || (received < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
      CloseConnection(worker, connection);
      return;
    }
    if (!StartRead(worker, connection)) CloseConnection(worker, connection);
    return;
  }
}

void HandleWrite(Worker* worker, Connection* connection) {
  const auto& response = coropact_bench::Response();
  while (connection->sent < response.size()) {
    const ssize_t written = ::send(connection->fd, response.data() + connection->sent,
                                   response.size() - connection->sent, MSG_NOSIGNAL);
    if (written > 0) {
      connection->sent += static_cast<std::size_t>(written);
      continue;
    }
    if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      if (!StartWrite(worker, connection)) CloseConnection(worker, connection);
      return;
    }
    CloseConnection(worker, connection);
    return;
  }

  connection->used = 0;
  connection->sent = 0;
  connection->writing = false;
  if (!StartRead(worker, connection)) CloseConnection(worker, connection);
}

void AcceptConnections(Worker* worker) {
  for (;;) {
    const int fd = ::accept4(worker->listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) break;
      break;
    }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    auto* connection = new Connection;
    connection->fd = fd;
    connection->worker = worker;
    connection->poll.worker = worker;
    connection->poll.connection = connection;
    worker->connections.insert(connection);
    if (!StartRead(worker, connection)) CloseConnection(worker, connection);
  }
  if (!SubmitPoll(worker, &worker->accept_poll, worker->listener, POLLIN)) {
    g_stop.store(true, std::memory_order_relaxed);
  }
}

void HandleEvent(Worker* worker, const io_event& event) {
  auto* operation = static_cast<PollOperation*>(event.data);
  if (operation == &worker->accept_poll) {
    AcceptConnections(worker);
    return;
  }
  auto* connection = operation->connection;
  if (connection == nullptr || worker->connections.find(connection) == worker->connections.end()) return;
  if (event.res < 0 || (event.res & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
    CloseConnection(worker, connection);
    return;
  }
  if (connection->writing) {
    HandleWrite(worker, connection);
  } else {
    HandleRead(worker, connection);
  }
}

void RunWorker(Worker* worker) {
  worker->listener = CreateListener(worker->port);
  if (worker->listener < 0 || ::io_setup(kAioEntries, &worker->context) < 0) {
    if (worker->listener >= 0) ::close(worker->listener);
    worker->listener = -1;
    g_stop.store(true, std::memory_order_relaxed);
    return;
  }

  worker->accept_poll.worker = worker;
  if (!SubmitPoll(worker, &worker->accept_poll, worker->listener, POLLIN)) {
    g_stop.store(true, std::memory_order_relaxed);
  }

  std::array<io_event, kMaxEvents> events{};
  while (!g_stop.load(std::memory_order_relaxed)) {
    timespec timeout{0, 100'000'000};
    const long count = ::io_getevents(worker->context, 1, kMaxEvents, events.data(), &timeout);
    if (count < 0) {
      if (-count == EINTR) continue;
      break;
    }
    for (long i = 0; i < count; ++i) HandleEvent(worker, events[static_cast<std::size_t>(i)]);
  }

  for (auto* connection : worker->connections) {
    ::close(connection->fd);
    delete connection;
  }
  worker->connections.clear();
  if (worker->listener >= 0) ::close(worker->listener);
  if (worker->context != nullptr) ::io_destroy(worker->context);
  worker->listener = -1;
  worker->context = nullptr;
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  const auto port = static_cast<std::uint16_t>(coropact_bench::EnvInt("PORT", 19090));
  const std::size_t workers = coropact_bench::EnvSize("LIBAIO_WORKERS", 4);
  if (port == 0 || workers == 0) return 2;

  std::vector<Worker> worker_state(workers);
  std::vector<std::thread> worker_threads;
  worker_threads.reserve(workers);
  for (auto& worker : worker_state) {
    worker.port = port;
    worker_threads.emplace_back(RunWorker, &worker);
  }
  std::printf("HttpLibaioBench bind=127.0.0.1 port=%u workers=%zu response_body=%zu\n", port,
              workers, coropact_bench::kResponseBodySize);
  std::fflush(stdout);
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  for (auto& thread : worker_threads) thread.join();
  return 0;
}
