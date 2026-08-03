// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// HTTP keep-alive benchmark for LUringRecvSource. Unlike the regular luring
// benchmark, every connection receives through recv_multishot with
// IOSQE_BUFFER_SELECT and an io_uring provided-buffer ring.

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include "coropact/io.h"
#include "coropact/luring/capability.h"
#include "coropact/luring/recv_source_stream.h"
#include "coropact/luring/server.h"
#include "coropact/net/endpoint.h"

namespace {

constexpr std::size_t kResponseBodySize = 512;
constexpr std::size_t kRequestBufferSize = 16 * 1024;
constexpr std::size_t kProvidedBufferSize = 4096;
constexpr std::size_t kProvidedBufferCapacity = 4;
constexpr std::size_t kDefaultSharedBufferCapacity = 1024;

std::atomic_bool g_stop{false};
std::atomic_uint64_t g_source_create_errors{0};
std::atomic_uint64_t g_read_errors{0};
std::atomic_uint64_t g_write_errors{0};
std::atomic_uint64_t g_close_errors{0};
std::atomic_int g_last_error{0};
std::atomic_int g_last_source_create_error{0};
std::atomic_int g_last_read_error{0};
std::atomic_int g_last_write_error{0};
std::atomic_int g_last_close_error{0};

void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

void RecordError(std::atomic_uint64_t& counter,
                 std::atomic_int& last,
                 int error) noexcept {
  counter.fetch_add(1, std::memory_order_relaxed);
  last.store(error, std::memory_order_relaxed);
  g_last_error.store(error, std::memory_order_relaxed);
}

int EnvInt(const char* key, int fallback) {
  const char* value = std::getenv(key);
  return value != nullptr ? std::atoi(value) : fallback;
}

std::size_t EnvSize(const char* key, std::size_t fallback) {
  const char* value = std::getenv(key);
  if (value == nullptr) return fallback;
  char* end = nullptr;
  const auto parsed = std::strtoull(value, &end, 10);
  return end != value ? static_cast<std::size_t>(parsed) : fallback;
}

const std::string& Response() {
  static const std::string response = [] {
    std::string value =
        "HTTP/1.1 200 OK\r\n"
        "Server: luring-multishot-buffer-bench\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 512\r\n"
        "Connection: keep-alive\r\n"
        "\r\n";
    value.append(kResponseBodySize, 'x');
    return value;
  }();
  return response;
}

bool HasHeaderTerminator(std::span<const std::byte> bytes) {
  for (std::size_t i = 3; i < bytes.size(); ++i) {
    if (bytes[i - 3] == std::byte{'\r'} && bytes[i - 2] == std::byte{'\n'} &&
        bytes[i - 1] == std::byte{'\r'} && bytes[i] == std::byte{'\n'}) {
      return true;
    }
  }
  return false;
}

coropact::coro::DetachedTask HttpSessionAdapter(
    coropact::luring::LUringStream stream) {
  coropact::luring::LUringRecvSourceOptions source_options;
  source_options.source.event_capacity = kProvidedBufferCapacity;
  source_options.source.buffer_capacity = kProvidedBufferCapacity;
  source_options.buffer_size = kProvidedBufferSize;

  auto source_stream = coropact::luring::LUringRecvSourceStream::Create(
      std::move(stream), source_options);
  if (!source_stream.has_value()) {
    RecordError(g_source_create_errors, g_last_source_create_error,
                source_stream.error().value());
    co_return;
  }
  auto session = std::move(*source_stream);

  std::array<std::byte, kRequestBufferSize> request{};
  const auto response =
      std::as_bytes(std::span(Response().data(), Response().size()));
  std::size_t used = 0;

  for (;;) {
    auto read = co_await session.ReadSome(std::span(request).subspan(used));
    if (!read.has_value()) {
      RecordError(g_read_errors, g_last_read_error, read.error().value());
      break;
    }
    if (*read == 0) break;

    used += *read;
    if (!HasHeaderTerminator(std::span<const std::byte>(request).first(used))) {
      if (used == request.size()) break;
      continue;
    }

    auto written = co_await coropact::io::WriteAll(session, response);
    if (!written.has_value()) {
      RecordError(g_write_errors, g_last_write_error, written.error().value());
      break;
    }
    used = 0;
  }

  auto closed = co_await session.Close();
  if (!closed.has_value()) {
    RecordError(g_close_errors, g_last_close_error, closed.error().value());
  }
}

coropact::coro::DetachedTask HttpSessionDirect(
    coropact::luring::LUringStream stream) {
  coropact::luring::LUringRecvSourceOptions source_options;
  source_options.source.event_capacity = kProvidedBufferCapacity;
  source_options.source.buffer_capacity = kProvidedBufferCapacity;
  source_options.buffer_size = kProvidedBufferSize;

  auto source_result = coropact::luring::LUringRecvSource::Create(
      stream.Loop(), stream.Fd(), source_options);
  if (!source_result.has_value()) {
    RecordError(g_source_create_errors, g_last_source_create_error,
                source_result.error().value());
    co_return;
  }
  auto source = std::move(*source_result);

  std::array<std::byte, kRequestBufferSize> request{};
  const auto response =
      std::as_bytes(std::span(Response().data(), Response().size()));
  std::size_t used = 0;

  for (;;) {
    auto next = co_await source.Next();
    if (!next.has_value()) {
      RecordError(g_read_errors, g_last_read_error, next.error().value());
      break;
    }
    if (!next->has_value()) break;

    const auto bytes = (*next)->buffer.Bytes();
    bool header_complete = used == 0 && HasHeaderTerminator(bytes);
    if (!header_complete) {
      if (bytes.size() > request.size() - used) break;
      std::memcpy(request.data() + used, bytes.data(), bytes.size());
      used += bytes.size();
      header_complete = HasHeaderTerminator(
          std::span<const std::byte>(request).first(used));
    }

    // The direct path releases the lease before issuing the response. The
    // application observes the provided bytes directly; only fragmented HTTP
    // headers use the fallback scratch buffer above.
    next->reset();
    if (!header_complete) continue;

    auto written = co_await coropact::io::WriteAll(stream, response);
    if (!written.has_value()) {
      RecordError(g_write_errors, g_last_write_error, written.error().value());
      break;
    }
    used = 0;
  }

  auto stopped = co_await source.Stop();
  if (!stopped.has_value()) {
    RecordError(g_close_errors, g_last_close_error, stopped.error().value());
  }
  auto closed = co_await stream.Close();
  if (!closed.has_value()) {
    RecordError(g_close_errors, g_last_close_error, closed.error().value());
  }
}

coropact::coro::DetachedTask HttpSession(
    coropact::luring::LUringStream stream,
    bool direct_consume) {
  if (direct_consume) {
    return HttpSessionDirect(std::move(stream));
  }
  return HttpSessionAdapter(std::move(stream));
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  const auto host = std::getenv("BIND_HOST") != nullptr
                        ? std::getenv("BIND_HOST")
                        : "127.0.0.1";
  const auto port = static_cast<std::uint16_t>(EnvInt("PORT", 19190));
  const std::size_t workers = EnvSize("URING_WORKERS", 4);
  const auto entries = static_cast<std::uint32_t>(EnvSize("URING_ENTRIES", 1024));
  const auto shared_buffer_capacity =
      EnvSize("SHARED_BUFFER_CAPACITY", kDefaultSharedBufferCapacity);
  const bool direct_consume =
      std::getenv("BUFFER_CONSUMER") != nullptr &&
      std::string_view(std::getenv("BUFFER_CONSUMER")) == "direct";

  if (workers == 0 || port == 0) {
    std::fprintf(stderr, "URING_WORKERS and PORT must be non-zero\n");
    return 2;
  }

  auto listen_addr = coropact::net::ParseIpAddress(host, port);
  if (!listen_addr.has_value()) {
    std::fprintf(stderr, "invalid BIND_HOST '%s': %s\n", host,
                 listen_addr.error().message().c_str());
    return 2;
  }

  coropact::luring::LUringOptions loop_options;
  loop_options.entries = entries;
  loop_options.shared_buffer_capacity = shared_buffer_capacity;
  loop_options.shared_buffer_size = kProvidedBufferSize;
  loop_options.active_profile =
      coropact::luring::RuntimeProfile::Core()
          .Require(coropact::luring::NativeFeature::kMultishotRecv)
          .Require(coropact::luring::NativeFeature::kProvidedBufferRing);

  coropact::luring::LUringServerOptions options;
  options.worker_group_options.worker_num = workers;
  options.worker_group_options.worker_options.loop_options = loop_options;
  options.worker_group_options.worker_options.accept_mode =
      coropact::luring::AcceptMode::kMultishot;
  options.worker_group_options.worker_options.listen_options.reuse_addr = true;
  options.worker_group_options.worker_options.listen_options.reuse_port = true;
  options.worker_group_options.worker_options.listen_options.accept_depth = 4;

  coropact::luring::LUringServer server(*listen_addr, std::move(options));
  server.SetSessionHandler(
      [direct_consume](coropact::luring::LUringWorkerContext&,
                                     coropact::luring::LUringStream stream) {
        return HttpSession(std::move(stream), direct_consume);
      });

  auto started = server.Start();
  if (!started.has_value()) {
    std::fprintf(stderr, "LUringServer::Start failed: %s\n",
                 started.error().message().c_str());
    return 1;
  }

  std::printf(
      "HttpLuringMultishotBench bind=%s port=%u workers=%zu entries=%u "
      "storage=mmap consumer=%s buffer_size=%zu buffer_capacity=%zu "
      "shared_buffer_capacity=%zu\n",
      host, port, workers, entries,
      direct_consume ? "direct" : "adapter",
      kProvidedBufferSize, kProvidedBufferCapacity, shared_buffer_capacity);
  std::fflush(stdout);

  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  server.Stop();
  std::printf("errors source_create=%llu read=%llu write=%llu close=%llu "
              "last_source_create=%d last_read=%d last_write=%d last_close=%d\n",
              static_cast<unsigned long long>(g_source_create_errors.load()),
              static_cast<unsigned long long>(g_read_errors.load()),
              static_cast<unsigned long long>(g_write_errors.load()),
              static_cast<unsigned long long>(g_close_errors.load()),
              g_last_source_create_error.load(), g_last_read_error.load(),
              g_last_write_error.load(), g_last_close_error.load());
  return 0;
}
