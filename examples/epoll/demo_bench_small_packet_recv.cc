// SPDX-License-Identifier: MIT
//
// Compare the backend-neutral epoll Stream::Read path with the Tokio
// current-thread small-packet benchmark. The server only receives and counts
// bytes; it does not echo them back.
//
//   cmake --build build-uring --target demo_bench_small_packet_epoll
//   PACKET=64 CONNECTIONS=16 DURATION_MS=3000 WARMUP_MS=200 \
//     build-uring/examples/epoll/demo_bench_small_packet_epoll

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <span>
#include <thread>
#include <vector>

#include "alyrn/coro/spawn.h"
#include "alyrn/epoll/listener.h"
#include "alyrn/epoll/loop.h"
#include "alyrn/epoll/stream.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/spawn.h"

namespace {

using alyrn::DetachedTask;
using alyrn::SpawnDetach;
using alyrn::epoll::Listener;
using alyrn::epoll::Loop;
using alyrn::epoll::Stream;

struct Stats {
  std::atomic<std::uint64_t> recvs{0};
  std::atomic<std::uint64_t> bytes{0};
  std::atomic<std::uint64_t> client_sends{0};
  std::atomic<std::uint64_t> client_bytes{0};
  std::atomic<std::uint64_t> errors{0};
};

std::uint64_t EnvU64(const char* name, std::uint64_t fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr) return fallback;
  char* end = nullptr;
  const auto parsed = std::strtoull(value, &end, 10);
  return end != value && *end == '\0' ? parsed : fallback;
}

int ConnectBlocking(std::uint16_t port) noexcept {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) return -1;

  int no_delay = 1;
  (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof(no_delay));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

void FloodClient(std::uint16_t port, std::size_t packet_size, std::chrono::milliseconds duration,
                 std::atomic_bool* failed, Stats* stats) noexcept {
  const int fd = ConnectBlocking(port);
  if (fd < 0) {
    failed->store(true, std::memory_order_relaxed);
    return;
  }

  std::vector<char> packet(packet_size, 'x');
  const auto deadline = std::chrono::steady_clock::now() + duration;
  while (std::chrono::steady_clock::now() < deadline) {
    const ssize_t written = ::send(fd, packet.data(), packet.size(), MSG_NOSIGNAL);
    if (written < 0) {
      if (errno == EINTR) continue;
      break;
    }
    if (written == 0) break;
    stats->client_sends.fetch_add(1, std::memory_order_relaxed);
    stats->client_bytes.fetch_add(static_cast<std::uint64_t>(written),
                                  std::memory_order_relaxed);
  }
  (void)::shutdown(fd, SHUT_WR);
  ::close(fd);
}

DetachedTask ReadSession(Stream stream, Stats* stats, std::size_t packet_size) {
  (void)stream.SetNoDelay(true);
  std::array<std::byte, 2048> buffer{};
  const auto destination = std::span(buffer).first(std::min(buffer.size(), packet_size));

  for (;;) {
    auto read = co_await stream.Read(destination);
    if (!read.has_value() || *read == 0) {
      if (!read.has_value()) stats->errors.fetch_add(1, std::memory_order_relaxed);
      break;
    }
    stats->recvs.fetch_add(1, std::memory_order_relaxed);
    stats->bytes.fetch_add(*read, std::memory_order_relaxed);
  }
  (void)co_await stream.Close();
}

DetachedTask AcceptLoop(Loop& loop, Listener& listener, Stats* stats, std::size_t packet_size) {
  for (;;) {
    auto accepted = co_await listener.Accept();
    if (!accepted.has_value()) co_return;
    SpawnDetach(loop, ReadSession(std::move(*accepted), stats, packet_size));
  }
}

}  // namespace

int main() {
  const auto packet_size = static_cast<std::size_t>(EnvU64("PACKET", 64));
  const auto connections = static_cast<std::size_t>(EnvU64("CONNECTIONS", 16));
  const auto duration = std::chrono::milliseconds(EnvU64("DURATION_MS", 3000));
  const auto warmup = std::chrono::milliseconds(EnvU64("WARMUP_MS", 200));
  if (packet_size == 0 || packet_size > 2048 || connections == 0 || duration.count() == 0) {
    std::fprintf(stderr, "PACKET must be in [1, 2048], CONNECTIONS and DURATION_MS > 0\n");
    return 2;
  }

  Stats stats;
  std::atomic_bool failed{false};
  std::atomic_bool ready{false};
  std::atomic<std::uint16_t> port{0};
  std::atomic<Loop*> loop_ptr{nullptr};

  std::thread server([&] {
    (void)::pthread_setname_np(::pthread_self(), "alyrn-server");
    Loop loop;
    auto listener_result = Listener::Create(&loop, alyrn::net::Endpoint::Loopback(0));
    if (!listener_result.has_value()) {
      failed.store(true, std::memory_order_relaxed);
      ready.store(true, std::memory_order_release);
      return;
    }
    auto listener = std::move(*listener_result);
    auto local = listener.LocalAddress();
    if (!local.has_value()) {
      failed.store(true, std::memory_order_relaxed);
      ready.store(true, std::memory_order_release);
      return;
    }
    SpawnDetach(loop, AcceptLoop(loop, listener, &stats, packet_size));
    port.store(local->ToPort(), std::memory_order_release);
    loop_ptr.store(&loop, std::memory_order_release);
    ready.store(true, std::memory_order_release);
    loop.Run();
  });

  while (!ready.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (failed.load(std::memory_order_relaxed) || port.load(std::memory_order_acquire) == 0) {
    server.join();
    return 1;
  }

  std::vector<std::thread> clients;
  clients.reserve(connections);
  const auto client_duration = duration + warmup;
  for (std::size_t i = 0; i < connections; ++i) {
    clients.emplace_back(FloodClient, port.load(std::memory_order_acquire), packet_size,
                         client_duration, &failed, &stats);
  }

  std::this_thread::sleep_for(warmup);
  stats.recvs.store(0, std::memory_order_relaxed);
  stats.bytes.store(0, std::memory_order_relaxed);
  stats.client_sends.store(0, std::memory_order_relaxed);
  stats.client_bytes.store(0, std::memory_order_relaxed);
  stats.errors.store(0, std::memory_order_relaxed);
  const auto measured = std::chrono::steady_clock::now();
  std::this_thread::sleep_for(duration);
  const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - measured).count();

  const auto recvs = stats.recvs.load(std::memory_order_relaxed);
  const auto bytes = stats.bytes.load(std::memory_order_relaxed);
  const auto client_sends = stats.client_sends.load(std::memory_order_relaxed);
  const auto client_bytes = stats.client_bytes.load(std::memory_order_relaxed);
  const auto errors = stats.errors.load(std::memory_order_relaxed);
  if (auto* loop = loop_ptr.load(std::memory_order_acquire); loop != nullptr) {
    loop->RequestStop();
  }
  for (auto& client : clients) client.join();
  server.join();

  const double mpps = static_cast<double>(recvs) / seconds / 1'000'000.0;
  const double mib_s = static_cast<double>(bytes) / seconds / (1024.0 * 1024.0);
  const double delivered = client_bytes > 0 ? static_cast<double>(bytes) / client_bytes : 0.0;
  std::printf(
      "EpollSmallPacketRecv packet=%zu connections=%zu duration_ms=%lld warmup_ms=%lld\n"
      "recvs=%llu bytes=%llu client_sends=%llu client_bytes=%llu deliver=%.3f errors=%llu "
      "failed=%d seconds=%.3f Mpps=%.3f MiB/s=%.1f\n",
      packet_size, connections, static_cast<long long>(duration.count()),
      static_cast<long long>(warmup.count()), static_cast<unsigned long long>(recvs),
      static_cast<unsigned long long>(bytes), static_cast<unsigned long long>(client_sends),
      static_cast<unsigned long long>(client_bytes), delivered,
      static_cast<unsigned long long>(errors), failed.load(std::memory_order_relaxed), seconds,
      mpps, mib_s);
  return failed.load(std::memory_order_relaxed) || errors != 0 ? 1 : 0;
}
