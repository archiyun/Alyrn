// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Raw TCP echo server built directly on vexo_luring.
//
// The session only performs io_uring accept/read/write operations. There is no
// HTTP parser, gateway, upstream connection, or application timer involved.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <memory_resource>
#include <span>
#include <thread>
#include <vector>

#if defined(VEXO_ENABLE_CTRACK)
#include <ctrack.hpp>
#endif

#include "vexo/coro/frame_allocator.h"
#include "vexo/luring/server.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/net_utils.h"

namespace {

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

const char* EnvOr(const char* key, const char* fallback) {
  const char* value = std::getenv(key);
  return value != nullptr ? value : fallback;
}

bool EnvBool(const char* key, bool fallback) {
  const char* value = std::getenv(key);
  return value != nullptr ? std::atoi(value) != 0 : fallback;
}

vexo::coro::Task<void> EchoSession(vexo::luring::LUringStream stream) {
  std::array<std::byte, 16 * 1024> buffer{};

  for (;;) {
    auto read = co_await stream.ReadSome(buffer);
    if (!read.has_value() || *read == 0) {
      break;
    }

    std::size_t written = 0;
    while (written < *read) {
      auto result = co_await stream.WriteSome(
          std::span<const std::byte>(buffer.data() + written, *read - written));
      if (!result.has_value() || *result == 0) {
        co_await stream.Close();
        co_return;
      }
      written += *result;
    }
  }

  co_await stream.Close();
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  const auto bind_host = EnvOr("BIND_HOST", "127.0.0.1");
  const auto port = static_cast<std::uint16_t>(EnvInt("PORT", 19090));
  const std::size_t workers = EnvSize("URING_WORKERS", 4);
  const auto entries = static_cast<std::uint32_t>(EnvSize("URING_ENTRIES", 8192));
  const auto accept_depth = std::max<std::size_t>(1, EnvSize("ACCEPT_DEPTH", 4));
  const auto ready_budget = EnvSize("MAX_READY_WORK_PER_TURN", 256);
  const bool frame_pool = EnvBool("FRAME_POOL", true);

  if (workers == 0) {
    std::fprintf(stderr, "URING_WORKERS must be greater than zero\n");
    return 1;
  }

  auto listen_addr = vexo::net::ParseIPv4Address(bind_host, port);
  if (!listen_addr.has_value()) {
    std::fprintf(stderr, "invalid BIND_HOST '%s': %s\n", bind_host,
                 listen_addr.error().message().c_str());
    return 1;
  }

  std::vector<std::unique_ptr<std::pmr::unsynchronized_pool_resource>> frame_pools;
  if (frame_pool) {
    frame_pools.reserve(workers);
    for (std::size_t i = 0; i < workers; ++i) {
      frame_pools.push_back(std::make_unique<std::pmr::unsynchronized_pool_resource>());
    }
  }

  vexo::luring::LUringServerOptions options;
  options.worker_group_options.worker_num = workers;
  options.worker_group_options.worker_options.loop_options.entries = entries;
  options.worker_group_options.worker_options.loop_options.max_ready_work_per_turn = ready_budget;
  options.worker_group_options.worker_options.listen_options.reuse_port = true;
  options.worker_group_options.worker_options.listen_options.accept_depth = accept_depth;
  options.worker_group_options.frame_resource_factory =
      [&frame_pools](std::size_t index) -> std::pmr::memory_resource* {
    return index < frame_pools.size() ? frame_pools[index].get() : nullptr;
  };

  vexo::luring::LUringServer server(*listen_addr, std::move(options));
  server.set_session_handler([](vexo::luring::LUringLoop&, vexo::luring::LUringStream stream) {
    return EchoSession(std::move(stream));
  });

  auto started = server.Start();
  if (!started.has_value()) {
    std::fprintf(stderr, "LUringServer::Start failed: %s\n", started.error().message().c_str());
    return 1;
  }

  std::printf(
      "RawEchoLUring bind=%s port=%u workers=%zu entries=%u accept_depth=%zu frame_pool=%s "
      "ready_budget=%zu\n",
      bind_host, port, workers, entries, accept_depth, frame_pool ? "on" : "off", ready_budget);
  std::fflush(stdout);

  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
#if defined(VEXO_ENABLE_CTRACK)
  std::fputs(ctrack::result_as_string().c_str(), stdout);
#endif
  server.Stop();
  return 0;
}
