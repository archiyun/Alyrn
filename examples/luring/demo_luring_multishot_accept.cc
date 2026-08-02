// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Minimal multishot-accept echo server.
//
// This example deliberately consumes LUringAcceptSource directly instead of
// using LUringWorker's built-in accept loop.  It demonstrates the important
// distinction between one logical Next() await and one physical multishot
// accept request, which may produce many CQEs before its terminal CQE.
//
// Run:
//   ./examples/luring/demo_luring_multishot_accept
//   printf 'hello\n' | nc 127.0.0.1 19091
//
// Environment:
//   BIND_HOST=127.0.0.1  PORT=19091  URING_ENTRIES=4096

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <print>
#include <span>
#include <thread>
#include <utility>

#include "coropact/coro/spawn.h"
#include "coropact/luring/worker.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/net_utils.h"

using namespace coropact;

namespace {

std::atomic_bool g_stop{false};

void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

int EnvInt(const char* key, int fallback) {
  const char* value = std::getenv(key);
  return value == nullptr ? fallback : std::atoi(value);
}

coro::DetachedTask EchoSession(luring::LUringStream stream) {
  std::array<std::byte, 16 * 1024> buffer{};

  for (;;) {
    auto read = co_await stream.ReadSome(buffer);
    if (!read.has_value() || *read == 0) {
      break;
    }

    std::size_t written = 0;
    while (written < *read) {
      auto write = co_await stream.WriteSome(
          std::span<const std::byte>(buffer.data() + written, *read - written));
      if (!write.has_value() || *write == 0) {
        co_await stream.Close();
        co_return;
      }
      written += *write;
    }
  }

  co_await stream.Close();
}

// Each Next() awaits exactly one logical accepted stream. Internally, the
// luring adapter first tries IORING_ACCEPT_MULTISHOT; CQEs with F_MORE enqueue
// more streams without resuming this coroutine until it calls Next() again.
// On kernels that reject multishot accept, the same source falls back to
// one-shot accept re-arm without changing this loop's semantics.
coro::DetachedTask RunAcceptSource(luring::LUringWorkerContext& context) {
  auto source_result = context.listener.AcceptSource({
      .pending_depth = 1,
      .event_capacity = 128,
  });
  if (!source_result.has_value()) {
    std::println(stderr, "AcceptSource creation failed: {}", source_result.error().message());
    co_return;
  }

  auto source = std::move(*source_result);
  for (;;) {
    auto accepted = co_await source.Next();
    if (!accepted.has_value()) {
      std::println(stderr, "AcceptSource terminal error: {}", accepted.error().message());
      break;
    }
    if (!*accepted) {
      break;
    }

    coro::SpawnDetach(context.loop, EchoSession(std::move(**accepted)));
  }

  // Source termination is graceful and idempotent.  Awaiting Stop() matters:
  // it waits until the target accept request and a possible cancel request
  // have both reached their terminal CQEs, so source destruction cannot leave
  // io_uring user_data pointing at its former frame.
  auto stopped = co_await source.Stop();
  if (!stopped.has_value()) {
    std::println(stderr, "AcceptSource stop failed: {}", stopped.error().message());
  }
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  const char* bind_host = std::getenv("BIND_HOST");
  if (bind_host == nullptr) {
    bind_host = "127.0.0.1";
  }
  const auto port = static_cast<std::uint16_t>(EnvInt("PORT", 19091));
  const auto entries = static_cast<std::uint32_t>(EnvInt("URING_ENTRIES", 4096));

  auto listen_address = coropact::net::ParseIpAddress(bind_host, port);
  if (!listen_address.has_value()) {
    std::println(stderr, "invalid BIND_HOST '{}': {}\n", bind_host,
                 listen_address.error().message());
    return 1;
  }

  coropact::luring::LUringWorkerOptions options;
  options.loop_options.entries = entries;
  options.listen_options.reuse_port = false;

  // No ConnectionCallback is passed to LUringWorker: the ThreadInitCallback
  // below owns the direct AcceptSource loop, avoiding a second accept mode on
  // the same listener.
  coropact::luring::LUringWorker worker(
      0, *listen_address, std::move(options), [](coropact::luring::LUringWorkerContext& context) {
        coropact::coro::SpawnDetach(context.loop, RunAcceptSource(context));
      });

  auto started = worker.Start();
  if (!started.has_value()) {
    std::println(stderr, "LUringWorker::Start failed: {}", started.error().message());
    return 1;
  }

  std::println(
      "multishot accept echo listening on {}:{} (one worker, {} SQ/CQ entries)\n"
      "press Ctrl-C to stop; unsupported kernels automatically use one-shot fallback",
      bind_host, port, entries);
  std::fflush(stdout);

  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  worker.Stop();
  return 0;
}
