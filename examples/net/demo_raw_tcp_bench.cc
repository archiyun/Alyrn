// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Small raw TCP request/response benchmark client.
//
// One payload is outstanding per persistent connection. The same epoll client
// is used against both raw echo servers, so the result measures the server's
// TCP accept/read/write path rather than HTTP or gateway work.

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

enum class State { kConnecting, kWriting, kReading, kFailed };

struct Connection {
  int fd{-1};
  State state{State::kFailed};
  std::size_t sent{0};
  std::size_t received{0};
  std::uint64_t request_started_ns{0};
};

struct Stats {
  std::uint64_t completed{0};
  std::uint64_t errors{0};
  std::vector<std::uint64_t> latency_ns;
};

std::uint64_t NowNs() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch())
          .count());
}

void Usage(const char* program) {
  std::fprintf(
      stderr, "usage: %s HOST PORT CONNECTIONS DURATION_SECONDS [WARMUP_SECONDS] [PAYLOAD_BYTES]\n",
      program);
}

bool SetNonBlocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool Modify(int epoll_fd, const Connection& connection, std::uint32_t events) {
  epoll_event event{};
  event.events = events;
  event.data.ptr = const_cast<Connection*>(&connection);
  return ::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, connection.fd, &event) == 0;
}

void CloseConnection(int epoll_fd, Connection& connection) {
  if (connection.fd < 0) return;
  ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, connection.fd, nullptr);
  ::close(connection.fd);
  connection.fd = -1;
  connection.state = State::kFailed;
}

bool StartWrite(int epoll_fd, Connection& connection, const std::vector<char>& payload) {
  connection.state = State::kWriting;
  connection.sent = 0;
  connection.received = 0;
  connection.request_started_ns = 0;
  return Modify(epoll_fd, connection, EPOLLOUT | EPOLLRDHUP | EPOLLERR | EPOLLHUP);
}

bool FlushWrite(int epoll_fd, Connection& connection, const std::vector<char>& payload) {
  while (connection.sent < payload.size()) {
    const ssize_t written = ::send(connection.fd, payload.data() + connection.sent,
                                   payload.size() - connection.sent, MSG_NOSIGNAL);
    if (written > 0) {
      if (connection.request_started_ns == 0) connection.request_started_ns = NowNs();
      connection.sent += static_cast<std::size_t>(written);
      continue;
    }
    if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return true;
    }
    return false;
  }

  connection.state = State::kReading;
  connection.received = 0;
  return Modify(epoll_fd, connection, EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP);
}

bool CompleteRequest(int epoll_fd, Connection& connection, const std::vector<char>& payload,
                     Stats& stats, std::uint64_t measurement_start_ns) {
  const std::uint64_t completed_at = NowNs();
  if (completed_at >= measurement_start_ns) {
    ++stats.completed;
    stats.latency_ns.push_back(completed_at - connection.request_started_ns);
  }
  return StartWrite(epoll_fd, connection, payload) && FlushWrite(epoll_fd, connection, payload);
}

bool HandleConnecting(int epoll_fd, Connection& connection, const std::vector<char>& payload) {
  int error = 0;
  socklen_t error_size = sizeof(error);
  if (::getsockopt(connection.fd, SOL_SOCKET, SO_ERROR, &error, &error_size) < 0 || error != 0) {
    return false;
  }
  return StartWrite(epoll_fd, connection, payload) && FlushWrite(epoll_fd, connection, payload);
}

bool HandleReading(int epoll_fd, Connection& connection, const std::vector<char>& payload,
                   Stats& stats, std::uint64_t measurement_start_ns) {
  char buffer[64 * 1024];
  for (;;) {
    const ssize_t received = ::recv(connection.fd, buffer, sizeof(buffer), 0);
    if (received > 0) {
      connection.received += static_cast<std::size_t>(received);
      if (connection.received >= payload.size()) {
        return CompleteRequest(epoll_fd, connection, payload, stats, measurement_start_ns);
      }
      continue;
    }
    if (received == 0) return false;
    if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
    return false;
  }
}

double PercentileMs(std::vector<std::uint64_t>& samples, double percentile) {
  if (samples.empty()) return 0.0;
  const std::size_t index = static_cast<std::size_t>(
      std::ceil((percentile / 100.0) * static_cast<double>(samples.size())));
  const std::size_t bounded = std::min(samples.size() - 1, index == 0 ? 0 : index - 1);
  std::nth_element(samples.begin(), samples.begin() + bounded, samples.end());
  return static_cast<double>(samples[bounded]) / 1'000'000.0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 5 || argc > 7) {
    Usage(argv[0]);
    return 2;
  }

  const char* host = argv[1];
  const int port = std::atoi(argv[2]);
  const int connection_count = std::atoi(argv[3]);
  const int duration_seconds = std::atoi(argv[4]);
  const int warmup_seconds = argc >= 6 ? std::atoi(argv[5]) : 2;
  const std::size_t payload_size = argc >= 7 ? std::strtoull(argv[6], nullptr, 10) : 64;
  if (port <= 0 || port > 65535 || connection_count <= 0 || duration_seconds <= 0 ||
      warmup_seconds < 0 || payload_size == 0) {
    Usage(argv[0]);
    return 2;
  }

  in_addr address{};
  if (::inet_pton(AF_INET, host, &address) != 1) {
    std::fprintf(stderr, "HOST must be a numeric IPv4 address: %s\n", host);
    return 2;
  }

  const int epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
  if (epoll_fd < 0) {
    std::perror("epoll_create1");
    return 1;
  }

  std::vector<char> payload(payload_size, 'x');
  std::vector<Connection> connections;
  connections.reserve(static_cast<std::size_t>(connection_count));
  sockaddr_in peer{};
  peer.sin_family = AF_INET;
  peer.sin_port = htons(static_cast<std::uint16_t>(port));
  peer.sin_addr = address;

  std::size_t failed_connections = 0;
  for (int i = 0; i < connection_count; ++i) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if (fd < 0 || !SetNonBlocking(fd)) {
      if (fd >= 0) ::close(fd);
      ++failed_connections;
      continue;
    }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    Connection connection;
    connection.fd = fd;
    const int result = ::connect(fd, reinterpret_cast<const sockaddr*>(&peer), sizeof(peer));
    if (result == 0) {
      connection.state = State::kWriting;
    } else if (errno == EINPROGRESS) {
      connection.state = State::kConnecting;
    } else {
      ::close(fd);
      ++failed_connections;
      continue;
    }

    epoll_event event{};
    event.events = EPOLLOUT | EPOLLRDHUP | EPOLLERR | EPOLLHUP;
    event.data.ptr = &connections.emplace_back(connection);
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) < 0) {
      CloseConnection(epoll_fd, connections.back());
      connections.pop_back();
      ++failed_connections;
    }
  }

  if (connections.empty()) {
    std::fprintf(stderr, "all connections failed (failed=%zu)\n", failed_connections);
    ::close(epoll_fd);
    return 1;
  }

  Stats stats;
  stats.latency_ns.reserve(1'000'000);
  const std::uint64_t warmup_end =
      NowNs() + static_cast<std::uint64_t>(warmup_seconds) * 1'000'000'000ULL;
  const std::uint64_t measurement_end =
      warmup_end + static_cast<std::uint64_t>(duration_seconds) * 1'000'000'000ULL;
  bool measurement_started = warmup_seconds == 0;
  std::uint64_t measurement_start = warmup_seconds == 0 ? NowNs() : 0;

  while (NowNs() < measurement_end) {
    epoll_event events[256];
    const int event_count = ::epoll_wait(epoll_fd, events, 256, 100);
    if (event_count < 0) {
      if (errno == EINTR) continue;
      std::perror("epoll_wait");
      break;
    }

    const std::uint64_t now = NowNs();
    if (!measurement_started && now >= warmup_end) {
      measurement_started = true;
      measurement_start = now;
      stats.completed = 0;
      stats.errors = 0;
      stats.latency_ns.clear();
    }

    for (int i = 0; i < event_count; ++i) {
      auto* connection = static_cast<Connection*>(events[i].data.ptr);
      if (connection->fd < 0) continue;

      bool ok = true;
      const std::uint32_t event = events[i].events;
      if ((event & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0) {
        ok = false;
      } else if (connection->state == State::kConnecting) {
        ok = HandleConnecting(epoll_fd, *connection, payload);
      } else if (connection->state == State::kWriting) {
        ok = FlushWrite(epoll_fd, *connection, payload);
      } else if (connection->state == State::kReading) {
        ok = HandleReading(epoll_fd, *connection, payload, stats, measurement_start);
      }

      if (!ok) {
        if (measurement_started) ++stats.errors;
        CloseConnection(epoll_fd, *connection);
      }
    }
  }

  const std::uint64_t end = NowNs();
  const double elapsed_seconds =
      measurement_start != 0 && end > measurement_start
          ? static_cast<double>(end - measurement_start) / 1'000'000'000.0
          : 0.0;
  const double rps =
      elapsed_seconds > 0.0 ? static_cast<double>(stats.completed) / elapsed_seconds : 0.0;
  const double p50 = PercentileMs(stats.latency_ns, 50.0);
  const double p99 = PercentileMs(stats.latency_ns, 99.0);
  std::fprintf(stdout,
               "connections=%zu failed_connect=%zu payload=%zu duration=%.3fs rps=%.2f "
               "completed=%llu errors=%llu p50_ms=%.3f p99_ms=%.3f\n",
               connections.size(), failed_connections, payload_size, elapsed_seconds, rps,
               static_cast<unsigned long long>(stats.completed),
               static_cast<unsigned long long>(stats.errors), p50, p99);

  for (auto& connection : connections) CloseConnection(epoll_fd, connection);
  ::close(epoll_fd);
  return stats.errors == 0 && failed_connections == 0 ? 0 : 1;
}
