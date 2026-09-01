// SPDX-License-Identifier: MIT
//
// Flood 64-byte TCP payloads at a uring Loop and compare single-shot
// Stream::Read against multishot RecvSource (provided-buffer recv).
//
//   cmake --build build-uring --target demo_bench_small_packet_recv
//   build-uring/examples/uring/demo_bench_small_packet_recv
//
//   PACKET=64 CONNECTIONS=16 DURATION_MS=3000 MODE=both
//     build-uring/examples/uring/demo_bench_small_packet_recv

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "alyrn/coro/spawn.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/spawn.h"
#include "alyrn/uring/listener.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/options.h"
#include "alyrn/uring/recv_source.h"
#include "alyrn/uring/stream.h"

namespace {

using alyrn::DetachedTask;
using alyrn::SpawnDetach;
using alyrn::uring::Listener;
using alyrn::uring::ListenOptions;
using alyrn::uring::Loop;
using alyrn::uring::Options;
using alyrn::uring::RecvSource;
using alyrn::uring::RecvSourceOptions;
using alyrn::uring::Stream;

enum class RecvMode : std::uint8_t {
  kSingleShot,
  kMultishot,
};

struct BenchConfig {
  std::size_t packet_size{64};
  std::size_t connections{16};
  std::uint64_t duration_ms{3000};
  std::uint64_t warmup_ms{200};
  std::uint32_t entries{4096};
  std::size_t shared_buffer_capacity{4096};
  std::size_t event_capacity{64};
  std::size_t buffer_capacity{64};
  alyrn::uring::TaskRunMode task_run_mode{alyrn::uring::TaskRunMode::kDefault};
};

struct BenchStats {
  std::atomic<std::uint64_t> recvs{0};
  std::atomic<std::uint64_t> bytes{0};
  std::atomic<std::uint64_t> errors{0};
  std::atomic<std::uint64_t> client_sends{0};
  std::atomic<std::uint64_t> client_bytes{0};
  std::atomic<int> first_error{0};
  std::atomic<std::uint64_t> create_fail{0};
  std::atomic<std::uint64_t> next_fail{0};
  std::atomic<std::uint64_t> next_eof{0};
};

struct RunResult {
  const char* mode{};
  std::uint64_t recvs{0};
  std::uint64_t bytes{0};
  std::uint64_t errors{0};
  std::uint64_t client_sends{0};
  std::uint64_t client_bytes{0};
  std::uint64_t create_fail{0};
  std::uint64_t next_fail{0};
  std::uint64_t next_eof{0};
  int first_error{0};
  double seconds{0};
};

std::uint64_t EnvU64(const char* key, std::uint64_t fallback) {
  const char* value = std::getenv(key);
  if (value == nullptr) {
    return fallback;
  }
  char* end = nullptr;
  const auto parsed = std::strtoull(value, &end, 10);
  return end != value && *end == '\0' ? parsed : fallback;
}

std::string_view EnvString(const char* key) {
  const char* value = std::getenv(key);
  return value != nullptr ? std::string_view(value) : std::string_view{};
}

bool PowerOfTwo(std::size_t value) noexcept { return value != 0 && (value & (value - 1)) == 0; }

int ConnectBlocking(std::uint16_t port) noexcept {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return -1;
  }

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
                 std::atomic_bool* failed, BenchStats* stats) noexcept {
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
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    if (written == 0) {
      break;
    }
    stats->client_sends.fetch_add(1, std::memory_order_relaxed);
    stats->client_bytes.fetch_add(static_cast<std::uint64_t>(written), std::memory_order_relaxed);
  }
  (void)::shutdown(fd, SHUT_WR);
  ::close(fd);
}

DetachedTask SingleShotSession(Stream stream, BenchStats* stats, std::size_t packet_size) {
  (void)stream.SetNoDelay(true);
  std::array<std::byte, 2048> buffer{};
  const auto dest = std::span(buffer).first(std::min(buffer.size(), packet_size));

  for (;;) {
    auto read = co_await stream.Read(dest);
    if (!read.has_value() || *read == 0) {
      if (!read.has_value()) {
        stats->errors.fetch_add(1, std::memory_order_relaxed);
      }
      break;
    }
    stats->recvs.fetch_add(1, std::memory_order_relaxed);
    stats->bytes.fetch_add(*read, std::memory_order_relaxed);
  }
  (void)co_await stream.Close();
}

DetachedTask MultishotSession(Stream stream, BenchStats* stats, std::size_t packet_size,
                              std::size_t event_capacity, std::size_t buffer_capacity) {
  RecvSourceOptions recv_options;
  recv_options.source.pending_depth = 1;
  recv_options.source.event_capacity = event_capacity;
  recv_options.source.buffer_capacity = buffer_capacity;
  recv_options.buffer_size = packet_size;

  auto source_result = RecvSource::Create(stream.OwnerLoop(), stream.Fd(), recv_options);
  if (!source_result.has_value()) {
    stats->create_fail.fetch_add(1, std::memory_order_relaxed);
    int expected = 0;
    stats->first_error.compare_exchange_strong(expected, source_result.error().value());
    stats->errors.fetch_add(1, std::memory_order_relaxed);
    (void)co_await stream.Close();
    co_return;
  }
  auto source = std::move(*source_result);

  for (;;) {
    auto received = co_await source.Next();
    if (!received.has_value()) {
      stats->next_fail.fetch_add(1, std::memory_order_relaxed);
      int expected = 0;
      stats->first_error.compare_exchange_strong(expected, received.error().value());
      stats->errors.fetch_add(1, std::memory_order_relaxed);
      break;
    }
    if (!received->has_value()) {
      stats->next_eof.fetch_add(1, std::memory_order_relaxed);
      break;
    }
    const std::size_t n = (*received)->buffer.Size();
    (*received)->buffer.Release();
    received->reset();
    stats->recvs.fetch_add(1, std::memory_order_relaxed);
    stats->bytes.fetch_add(n, std::memory_order_relaxed);
  }
  (void)co_await source.Stop();
  (void)co_await stream.Close();
}

DetachedTask AcceptLoop(Loop& loop, Listener& listener, RecvMode mode, BenchStats* stats,
                        const BenchConfig& config) {
  for (;;) {
    auto accepted = co_await listener.Accept();
    if (!accepted.has_value()) {
      co_return;
    }
    if (mode == RecvMode::kMultishot) {
      SpawnDetach(loop, MultishotSession(std::move(*accepted), stats, config.packet_size,
                                         config.event_capacity, config.buffer_capacity));
    } else {
      SpawnDetach(loop, SingleShotSession(std::move(*accepted), stats, config.packet_size));
    }
  }
}

RunResult RunOnce(const BenchConfig& config, RecvMode mode) {
  BenchStats stats;
  std::atomic_bool clients_failed{false};
  std::atomic_bool ready{false};
  std::atomic<std::uint16_t> port{0};
  std::atomic<Loop*> loop_ptr{nullptr};

  std::thread server([&] {
    Loop loop;
    Options options;
    options.entries = config.entries;
    options.shared_buffer_capacity =
        mode == RecvMode::kMultishot ? config.shared_buffer_capacity : 0;
    options.shared_buffer_size = config.packet_size;
    options.task_run_mode = config.task_run_mode;

    auto initialized = loop.Init(options);
    if (!initialized.has_value()) {
      clients_failed.store(true, std::memory_order_relaxed);
      ready.store(true, std::memory_order_release);
      return;
    }

    ListenOptions listen_options;
    listen_options.reuse_addr = true;
    listen_options.reuse_port = false;
    listen_options.tcp_options.no_delay = true;

    auto listener_result =
        Listener::Create(&loop, alyrn::net::Endpoint::Loopback(0), listen_options);
    if (!listener_result.has_value()) {
      clients_failed.store(true, std::memory_order_relaxed);
      ready.store(true, std::memory_order_release);
      return;
    }
    auto listener = std::move(*listener_result);
    auto local = listener.LocalAddress();
    if (!local.has_value()) {
      clients_failed.store(true, std::memory_order_relaxed);
      ready.store(true, std::memory_order_release);
      return;
    }

    SpawnDetach(loop, AcceptLoop(loop, listener, mode, &stats, config));
    port.store(local->ToPort(), std::memory_order_release);
    loop_ptr.store(&loop, std::memory_order_release);
    ready.store(true, std::memory_order_release);
    loop.Run();
    (void)listener;
  });

  while (!ready.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  RunResult result;
  result.mode = mode == RecvMode::kMultishot ? "multishot" : "single-shot";
  const auto listen_port = port.load(std::memory_order_acquire);
  if (clients_failed.load(std::memory_order_relaxed) || listen_port == 0) {
    if (auto* loop = loop_ptr.load(std::memory_order_acquire); loop != nullptr) {
      loop->RequestStop();
    }
    server.join();
    result.errors = 1;
    return result;
  }

  const auto duration = std::chrono::milliseconds(config.duration_ms + config.warmup_ms);
  std::vector<std::thread> clients;
  clients.reserve(config.connections);
  for (std::size_t i = 0; i < config.connections; ++i) {
    clients.emplace_back(FloodClient, listen_port, config.packet_size, duration, &clients_failed,
                         &stats);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(config.warmup_ms));
  stats.recvs.store(0, std::memory_order_relaxed);
  stats.bytes.store(0, std::memory_order_relaxed);
  stats.errors.store(0, std::memory_order_relaxed);
  stats.client_sends.store(0, std::memory_order_relaxed);
  stats.client_bytes.store(0, std::memory_order_relaxed);
  const auto measured = std::chrono::steady_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(config.duration_ms));
  result.recvs = stats.recvs.load(std::memory_order_relaxed);
  result.bytes = stats.bytes.load(std::memory_order_relaxed);
  result.errors = stats.errors.load(std::memory_order_relaxed);
  result.client_sends = stats.client_sends.load(std::memory_order_relaxed);
  result.client_bytes = stats.client_bytes.load(std::memory_order_relaxed);
  result.create_fail = stats.create_fail.load(std::memory_order_relaxed);
  result.next_fail = stats.next_fail.load(std::memory_order_relaxed);
  result.next_eof = stats.next_eof.load(std::memory_order_relaxed);
  result.first_error = stats.first_error.load(std::memory_order_relaxed);
  result.seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - measured).count();

  if (auto* loop = loop_ptr.load(std::memory_order_acquire); loop != nullptr) {
    loop->RequestStop();
  }
  for (auto& client : clients) {
    client.join();
  }
  server.join();
  if (clients_failed.load(std::memory_order_relaxed)) {
    ++result.errors;
  }
  return result;
}

void PrintResult(const RunResult& result) {
  const double mpps =
      result.seconds > 0 ? (static_cast<double>(result.recvs) / result.seconds) / 1'000'000.0 : 0;
  const double mib_s =
      result.seconds > 0 ? (static_cast<double>(result.bytes) / result.seconds) / (1024.0 * 1024.0)
                         : 0;
  const double delivered = result.client_bytes > 0 ? static_cast<double>(result.bytes) /
                                                         static_cast<double>(result.client_bytes)
                                                   : 0;
  std::printf(
      "%-12s recvs=%llu bytes=%llu client_sends=%llu client_bytes=%llu "
      "deliver=%.3f errors=%llu create_fail=%llu next_fail=%llu next_eof=%llu "
      "first_errno=%d seconds=%.3f Mpps=%.3f MiB/s=%.1f\n",
      result.mode, static_cast<unsigned long long>(result.recvs),
      static_cast<unsigned long long>(result.bytes),
      static_cast<unsigned long long>(result.client_sends),
      static_cast<unsigned long long>(result.client_bytes), delivered,
      static_cast<unsigned long long>(result.errors),
      static_cast<unsigned long long>(result.create_fail),
      static_cast<unsigned long long>(result.next_fail),
      static_cast<unsigned long long>(result.next_eof), result.first_error, result.seconds, mpps,
      mib_s);
}

}  // namespace

int main() {
  BenchConfig config;
  config.packet_size = static_cast<std::size_t>(EnvU64("PACKET", 64));
  config.connections = static_cast<std::size_t>(EnvU64("CONNECTIONS", 16));
  config.duration_ms = EnvU64("DURATION_MS", 3000);
  config.warmup_ms = EnvU64("WARMUP_MS", 200);
  config.entries = static_cast<std::uint32_t>(EnvU64("URING_ENTRIES", 4096));
  config.shared_buffer_capacity = static_cast<std::size_t>(EnvU64("SHARED_BUFFER_CAPACITY", 4096));
  config.event_capacity = static_cast<std::size_t>(EnvU64("EVENT_CAPACITY", 8));
  config.buffer_capacity = static_cast<std::size_t>(EnvU64("BUFFER_CAPACITY", 8));

  const auto task_run_mode = EnvString("TASK_RUN_MODE");
  if (task_run_mode.empty() || task_run_mode == "default") {
    config.task_run_mode = alyrn::uring::TaskRunMode::kDefault;
  } else if (task_run_mode == "coop" || task_run_mode == "cooperative") {
    config.task_run_mode = alyrn::uring::TaskRunMode::kCooperative;
  } else if (task_run_mode == "defer" || task_run_mode == "deferred") {
    config.task_run_mode = alyrn::uring::TaskRunMode::kDeferred;
  } else {
    std::fprintf(stderr, "TASK_RUN_MODE must be default, cooperative, or deferred\n");
    return 2;
  }

  const auto mode = EnvString("MODE");
  const bool run_single =
      mode.empty() || mode == "both" || mode == "single" || mode == "single-shot";
  const bool run_multi = mode.empty() || mode == "both" || mode == "multi" || mode == "multishot";

  if (config.packet_size == 0 || config.connections == 0 || config.duration_ms == 0 ||
      config.entries == 0 || config.event_capacity == 0 || config.buffer_capacity == 0 ||
      config.buffer_capacity < config.event_capacity ||
      !PowerOfTwo(config.shared_buffer_capacity) || config.shared_buffer_capacity > 32 * 1024) {
    std::fprintf(stderr,
                 "PACKET, CONNECTIONS, DURATION_MS, URING_ENTRIES, EVENT_CAPACITY must be > 0; "
                 "BUFFER_CAPACITY >= EVENT_CAPACITY; "
                 "SHARED_BUFFER_CAPACITY must be a power of two in [1, 32768]\n");
    return 2;
  }

  std::printf(
      "SmallPacketRecv packet=%zu connections=%zu duration_ms=%llu warmup_ms=%llu "
      "entries=%u shared_buffer_capacity=%zu event_capacity=%zu buffer_capacity=%zu "
      "task_run_mode=%s\n",
      config.packet_size, config.connections, static_cast<unsigned long long>(config.duration_ms),
      static_cast<unsigned long long>(config.warmup_ms), config.entries,
      config.shared_buffer_capacity, config.event_capacity, config.buffer_capacity,
      config.task_run_mode == alyrn::uring::TaskRunMode::kDefault
          ? "default"
          : config.task_run_mode == alyrn::uring::TaskRunMode::kCooperative ? "cooperative"
                                                                             : "deferred");
  std::fflush(stdout);

  std::optional<RunResult> single;
  std::optional<RunResult> multi;
  if (run_single) {
    single = RunOnce(config, RecvMode::kSingleShot);
    PrintResult(*single);
    std::fflush(stdout);
  }
  if (run_multi) {
    multi = RunOnce(config, RecvMode::kMultishot);
    PrintResult(*multi);
    std::fflush(stdout);
  }

  if (single.has_value() && multi.has_value() && single->recvs > 0) {
    const double recv_ratio =
        static_cast<double>(multi->recvs) / static_cast<double>(single->recvs);
    const double byte_ratio =
        single->bytes > 0 ? static_cast<double>(multi->bytes) / static_cast<double>(single->bytes)
                          : 0;
    std::printf("ratio multishot/single-shot recvs=%.3f bytes=%.3f\n", recv_ratio, byte_ratio);
  }
  return (single.has_value() && single->errors != 0) || (multi.has_value() && multi->errors != 0)
             ? 1
             : 0;
}
