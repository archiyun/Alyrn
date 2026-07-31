// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Fixed HTTP keep-alive benchmark server for the CoroPact luring backend.
// The workload deliberately includes request-header framing and a fixed
// response body so it is closer to a small real service than a TCP echo.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <memory>
#include <memory_resource>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "coropact/coro/frame_allocator.h"
#include "coropact/io.h"
#include "coropact/luring/server.h"
#include "coropact/net/endpoint.h"

namespace {

constexpr std::size_t kResponseBodySize = 512;
constexpr std::size_t kRequestBufferSize = 16 * 1024;

std::atomic_bool g_stop{false};

void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

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

bool EnvBool(const char* key, bool fallback) {
  const char* value = std::getenv(key);
  return value != nullptr ? std::atoi(value) != 0 : fallback;
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

bool HasHeaderTerminator(std::span<const std::byte> bytes) {
  for (std::size_t i = 3; i < bytes.size(); ++i) {
    if (bytes[i - 3] == std::byte{'\r'} && bytes[i - 2] == std::byte{'\n'} &&
        bytes[i - 1] == std::byte{'\r'} && bytes[i] == std::byte{'\n'}) {
      return true;
    }
  }
  return false;
}

coropact::coro::DetachedTask HttpSession(coropact::luring::LUringStream stream) {
  std::array<std::byte, kRequestBufferSize> request{};
  const auto response = std::as_bytes(std::span(Response().data(), Response().size()));
  std::size_t used = 0;

  for (;;) {
    auto read = co_await stream.ReadSome(std::span(request).subspan(used));
    if (!read.has_value() || *read == 0) break;

    used += *read;
    if (!HasHeaderTerminator(std::span<const std::byte>(request).first(used))) {
      if (used == request.size()) break;
      continue;
    }

    auto written = co_await coropact::io::WriteAll(stream, response);
    if (!written.has_value()) break;

    // wrk does not pipeline by default. Resetting here keeps the benchmark's
    // one-request-at-a-time keep-alive semantics explicit.
    used = 0;
  }

  co_await stream.Close();
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  const auto host = std::getenv("BIND_HOST") != nullptr ? std::getenv("BIND_HOST") : "127.0.0.1";
  const auto port = static_cast<std::uint16_t>(EnvInt("PORT", 19090));
  const std::size_t workers = EnvSize("URING_WORKERS", 4);
  const auto entries = static_cast<std::uint32_t>(EnvSize("URING_ENTRIES", 8192));
  const bool frame_pool = EnvBool("FRAME_POOL", true);

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
  auto binding = coropact::luring::BindLUring(loop_options,
                                              coropact::luring::RuntimeProfile::Core());
  if (!binding.has_value()) {
    std::fprintf(stderr, "BindLUring failed: %s\n", binding.error().message().c_str());
    return 1;
  }

  std::vector<std::unique_ptr<coropact::coro::CoroFramePoolResource>> frame_pools;
  if (frame_pool) {
    frame_pools.reserve(workers);
    for (std::size_t i = 0; i < workers; ++i) {
      frame_pools.push_back(std::make_unique<coropact::coro::CoroFramePoolResource>());
    }
  }

  coropact::luring::LUringServerOptions options;
  options.worker_group_options.worker_num = workers;
  options.worker_group_options.worker_options.loop_options = loop_options;
  options.worker_group_options.worker_options.listen_options.reuse_addr = true;
  options.worker_group_options.worker_options.listen_options.reuse_port = true;
  options.worker_group_options.worker_options.listen_options.accept_depth =
      std::max<std::size_t>(1, EnvSize("ACCEPT_DEPTH", 4));
  options.worker_group_options.frame_resource_factory =
      [&frame_pools](std::size_t index) -> std::pmr::memory_resource* {
    return index < frame_pools.size() ? frame_pools[index].get() : nullptr;
  };

  coropact::luring::LUringServer server(*listen_addr, std::move(options));
  server.SetSessionHandler(
      [](coropact::luring::LUringWorkerContext&, coropact::luring::LUringStream stream) {
    return HttpSession(std::move(stream));
  });

  auto started = server.Start();
  if (!started.has_value()) {
    std::fprintf(stderr, "LUringServer::Start failed: %s\n", started.error().message().c_str());
    return 1;
  }

  std::printf("HttpLuringBench bind=%s port=%u workers=%zu entries=%u accept_depth=%zu "
              "frame_pool=%s response_body=%zu\n",
              host, port, workers, entries,
              options.worker_group_options.worker_options.listen_options.accept_depth,
              frame_pool ? "on" : "off", kResponseBodySize);
  std::fflush(stdout);

  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  server.Stop();
  return 0;
}
