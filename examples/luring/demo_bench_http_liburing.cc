// SPDX-License-Identifier: MIT
//
// Native liburing fixed HTTP keep-alive benchmark server. It mirrors the
// luring benchmark's thread-per-ring topology without using Alyrn runtime
// objects, and is kept as an abstraction-cost reference.

#include <arpa/inet.h>
#include <errno.h>
#include <liburing.h>
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
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace {

constexpr std::size_t kResponseBodySize = 512;
constexpr std::size_t kRequestBufferSize = 16 * 1024;
constexpr std::size_t kAcceptDepth = 4;

std::atomic_bool g_stop{false};

void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

int EnvInt(const char* key, int fallback) {
  const char* value = std::getenv(key);
  return value != nullptr ? std::atoi(value) : fallback;
}

std::size_t EnvSize(const char* key, std::size_t fallback) {
  const char* value = std::getenv(key);
  if (value == nullptr) {
    return fallback;
  }
  char* end = nullptr;
  const auto parsed = std::strtoull(value, &end, 10);
  return end != value ? static_cast<std::size_t>(parsed) : fallback;
}

const std::string& Response() {
  static const std::string response = [] {
    std::string value =
        "HTTP/1.1 200 OK\r\n"
        "Server: unified-http-bench\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 512\r\n"
        "Connection: keep-alive\r\n"
        "\r\n";
    value.append(kResponseBodySize, 'x');
    return value;
  }();
  return response;
}

bool HasHeaderTerminator(const char* bytes, std::size_t size) {
  for (std::size_t i = 3; i < size; ++i) {
    if (bytes[i - 3] == '\r' && bytes[i - 2] == '\n' && bytes[i - 1] == '\r' && bytes[i] == '\n') {
      return true;
    }
  }
  return false;
}

int CreateListener(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return -1;
  }

  int one = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0 ||
      ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0 ||
      ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
    ::close(fd);
    return -1;
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 ||
      ::listen(fd, 4096) < 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

struct Connection {
  explicit Connection(int socket) : fd(socket) {}

  int fd{-1};
  std::array<char, kRequestBufferSize> request{};
  std::size_t request_used{0};
  std::size_t response_sent{0};
};

enum class OperationKind {
  kAccept,
  kRead,
  kWrite,
};

struct Operation {
  OperationKind kind;
  Connection* connection{nullptr};
};

class Worker {
public:
  Worker(std::size_t index, std::uint16_t port, unsigned entries)
      : index_(index), port_(port), entries_(entries) {}

  void Run() {
    listener_ = CreateListener(port_);
    if (listener_ < 0) {
      std::fprintf(stderr, "raw liburing worker %zu: listener failed: %s\n", index_,
                   std::strerror(errno));
      return;
    }

    const int init_result = io_uring_queue_init(entries_, &ring_, 0);
    if (init_result < 0) {
      std::fprintf(stderr, "raw liburing worker %zu: queue init failed: %s\n", index_,
                   std::strerror(-init_result));
      ::close(listener_);
      listener_ = -1;
      return;
    }
    initialized_ = true;

    EnsureAcceptDepth();
    while (!g_stop.load(std::memory_order_relaxed)) {
      FlushDeferred();
      if (io_uring_submit(&ring_) < 0) {
        break;
      }

      io_uring_cqe* cqe = nullptr;
      __kernel_timespec timeout{0, 100'000'000};
      const int wait_result = io_uring_wait_cqe_timeout(&ring_, &cqe, &timeout);
      if (wait_result == -ETIME || wait_result == -EINTR) {
        continue;
      }
      if (wait_result < 0) {
        break;
      }

      Process(cqe);
      io_uring_cqe_seen(&ring_, cqe);

      while (io_uring_peek_cqe(&ring_, &cqe) == 0) {
        Process(cqe);
        io_uring_cqe_seen(&ring_, cqe);
      }
    }

    Cleanup();
  }

private:
  void Destroy(Operation* operation) {
    pending_operations_.erase(operation);
    delete operation;
  }

  bool TryPrepare(Operation* operation) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      return false;
    }

    switch (operation->kind) {
      case OperationKind::kAccept:
        io_uring_prep_accept(sqe, listener_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        break;
      case OperationKind::kRead:
        io_uring_prep_recv(
            sqe, operation->connection->fd,
            operation->connection->request.data() + operation->connection->request_used,
            operation->connection->request.size() - operation->connection->request_used, 0);
        break;
      case OperationKind::kWrite: {
        const auto& response = Response();
        io_uring_prep_send(sqe, operation->connection->fd,
                           response.data() + operation->connection->response_sent,
                           response.size() - operation->connection->response_sent, MSG_NOSIGNAL);
        break;
      }
    }

    io_uring_sqe_set_data(sqe, operation);
    return true;
  }

  void Schedule(OperationKind kind, Connection* connection) {
    auto* operation = new Operation{kind, connection};
    pending_operations_.insert(operation);
    // Once an SQE backlog exists, completed connections must not bypass it.
    // Otherwise their immediate re-submissions continually consume newly
    // available SQEs and leave older connections deferred indefinitely.
    if (!deferred_operations_.empty() || !TryPrepare(operation)) {
      deferred_operations_.push_back(operation);
    }
  }

  void FlushDeferred() {
    while (!deferred_operations_.empty()) {
      Operation* operation = deferred_operations_.front();
      if (operation->connection != nullptr &&
          connections_.find(operation->connection) == connections_.end()) {
        deferred_operations_.pop_front();
        Destroy(operation);
        continue;
      }
      if (!TryPrepare(operation)) {
        return;
      }
      deferred_operations_.pop_front();
    }
  }

  void EnsureAcceptDepth() {
    while (!g_stop.load(std::memory_order_relaxed) && accept_operations_ < kAcceptDepth) {
      ++accept_operations_;
      Schedule(OperationKind::kAccept, nullptr);
    }
  }

  void ScheduleRead(Connection* connection) {
    if (connection->request_used == connection->request.size()) {
      Close(connection);
      return;
    }
    Schedule(OperationKind::kRead, connection);
  }

  void ScheduleWrite(Connection* connection) { Schedule(OperationKind::kWrite, connection); }

  void Process(io_uring_cqe* cqe) {
    auto* operation = static_cast<Operation*>(io_uring_cqe_get_data(cqe));
    if (operation == nullptr) {
      return;
    }
    const int result = cqe->res;
    const auto kind = operation->kind;
    Connection* connection = operation->connection;
    if (kind == OperationKind::kAccept) {
      --accept_operations_;
    }
    Destroy(operation);

    if (kind == OperationKind::kAccept) {
      if (result >= 0) {
        int one = 1;
        (void)::setsockopt(result, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        auto* accepted = new Connection(result);
        connections_.insert(accepted);
        ScheduleRead(accepted);
      }
      if (!g_stop.load(std::memory_order_relaxed)) {
        EnsureAcceptDepth();
      }
      return;
    }

    if (connection == nullptr || connections_.find(connection) == connections_.end()) {
      return;
    }
    if (result <= 0) {
      Close(connection);
      return;
    }

    if (kind == OperationKind::kRead) {
      connection->request_used += static_cast<std::size_t>(result);
      if (!HasHeaderTerminator(connection->request.data(), connection->request_used)) {
        ScheduleRead(connection);
        return;
      }
      connection->response_sent = 0;
      ScheduleWrite(connection);
      return;
    }

    connection->response_sent += static_cast<std::size_t>(result);
    if (connection->response_sent < Response().size()) {
      ScheduleWrite(connection);
      return;
    }
    connection->request_used = 0;
    connection->response_sent = 0;
    ScheduleRead(connection);
  }

  void Close(Connection* connection) {
    if (connections_.erase(connection) == 0) {
      return;
    }
    (void)::close(connection->fd);
    delete connection;
  }

  void Cleanup() {
    for (auto* connection : connections_) {
      (void)::close(connection->fd);
      delete connection;
    }
    connections_.clear();
    deferred_operations_.clear();
    for (auto* operation : pending_operations_) {
      delete operation;
    }
    pending_operations_.clear();
    if (initialized_) {
      io_uring_queue_exit(&ring_);
      initialized_ = false;
    }
    if (listener_ >= 0) {
      (void)::close(listener_);
      listener_ = -1;
    }
  }

  std::size_t index_;
  std::uint16_t port_;
  unsigned entries_;
  int listener_{-1};
  io_uring ring_{};
  bool initialized_{false};
  std::size_t accept_operations_{0};
  std::unordered_set<Connection*> connections_;
  std::unordered_set<Operation*> pending_operations_;
  std::deque<Operation*> deferred_operations_;
};

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  const auto port = static_cast<std::uint16_t>(EnvInt("PORT", 19090));
  const std::size_t workers = EnvSize("URING_WORKERS", 4);
  const auto entries = static_cast<unsigned>(EnvSize("URING_ENTRIES", 8192));
  if (port == 0 || workers == 0 || entries == 0) {
    std::fprintf(stderr, "PORT, URING_WORKERS and URING_ENTRIES must be non-zero\n");
    return 2;
  }

  std::printf(
      "HttpRawLiburingBench bind=127.0.0.1 port=%u workers=%zu entries=%u "
      "response_body=%zu\n",
      port, workers, entries, kResponseBodySize);
  std::fflush(stdout);

  std::vector<std::thread> worker_threads;
  worker_threads.reserve(workers);
  for (std::size_t i = 0; i < workers; ++i) {
    worker_threads.emplace_back([i, port, entries] { Worker(i, port, entries).Run(); });
  }
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  for (auto& thread : worker_threads) {
    thread.join();
  }
  return 0;
}
