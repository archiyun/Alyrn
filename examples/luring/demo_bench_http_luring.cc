// SPDX-License-Identifier: MIT

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <span>
#include <string_view>
#include <thread>
#include <utility>

#include "bench_http_common.h"
#include "coropact/io.h"
#include "coropact/luring/detail/worker_group.h"
#include "coropact/luring/recv_source.h"
#include "coropact/net/endpoint.h"

namespace {

std::atomic_bool g_stop{false};

void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

[[nodiscard]]
bool EnvEnabled(const char* key, bool fallback) noexcept {
  return coropact_bench::EnvInt(key, fallback ? 1 : 0) != 0;
}

[[nodiscard]]
bool AcceptMultishotEnabled() noexcept {
  const auto mode = coropact_bench::EnvString("ACCEPT_MODE");
  if (mode.empty()) {
    return true;
  }
  return mode != "single" && mode != "single-shot";
}

// Consume one HTTP request (headers + Content-Length body), then WriteAll the
// fixed response. Leftover pipelined bytes stay in `request`/`used`.
coropact::coro::DetachedTask HttpSessionReadSome(coropact::luring::Stream stream) {
  std::array<std::byte, coropact_bench::kRequestBufferSize> request{};
  const auto response = std::as_bytes(
      std::span(coropact_bench::Response().data(), coropact_bench::Response().size()));
  std::size_t used = 0;
  std::size_t body_remain = 0;
  bool reading_body = false;

  for (;;) {
    if (!reading_body) {
      auto read = co_await stream.ReadSome(std::span(request).subspan(used));
      if (!read.has_value() || *read == 0) {
        break;
      }
      used += *read;

      const auto header_end = coropact_bench::HeaderTerminatorEnd(
          reinterpret_cast<const char*>(request.data()), used);
      if (header_end == static_cast<std::size_t>(-1)) {
        if (used == request.size()) {
          break;
        }
        continue;
      }

      const std::size_t content_length = coropact_bench::ParseContentLength(
          reinterpret_cast<const char*>(request.data()), header_end);
      const std::size_t available = used - header_end;
      if (available >= content_length) {
        const std::size_t consumed = header_end + content_length;
        const std::size_t remain = used - consumed;
        auto written = co_await stream.WriteAll(response);
        if (!written.has_value()) {
          break;
        }
        if (remain > 0) {
          std::memmove(request.data(), request.data() + consumed, remain);
        }
        used = remain;
        continue;
      }

      body_remain = content_length - available;
      used = 0;
      reading_body = true;
      continue;
    }

    auto read = co_await stream.ReadSome(std::span(request));
    if (!read.has_value() || *read == 0) {
      break;
    }
    if (*read >= body_remain) {
      const std::size_t leftover = *read - body_remain;
      if (leftover > 0) {
        std::memmove(request.data(), request.data() + body_remain, leftover);
      }
      used = leftover;
      body_remain = 0;
      reading_body = false;
      auto written = co_await stream.WriteAll(response);
      if (!written.has_value()) {
        break;
      }
    } else {
      body_remain -= *read;
    }
  }
  (void)co_await stream.Close();
}

coropact::coro::DetachedTask HttpSessionRecvSource(coropact::luring::Stream stream) {
  const auto response = std::as_bytes(
      std::span(coropact_bench::Response().data(), coropact_bench::Response().size()));

  coropact::luring::RecvSourceOptions recv_options;
  recv_options.source.pending_depth = 1;
  recv_options.source.event_capacity = 16;
  recv_options.source.buffer_capacity = 16;
  recv_options.buffer_size = coropact_bench::EnvSize(
      "SHARED_BUFFER_SIZE", coropact_bench::kRequestBufferSize);

  auto source_result =
      coropact::luring::RecvSource::Create(stream.OwnerLoop(), stream.Fd(), recv_options);
  if (!source_result.has_value()) {
    (void)co_await stream.Close();
    co_return;
  }
  auto source = std::move(*source_result);

  std::array<std::byte, coropact_bench::kRequestBufferSize> staging{};
  std::size_t used = 0;
  std::size_t body_remain = 0;
  bool reading_body = false;
  bool failed = false;

  for (;;) {
    auto received = co_await source.Next();
    if (!received.has_value()) {
      failed = true;
      break;
    }
    if (!received->has_value()) {
      break;
    }

    auto bytes = (*received)->buffer.Bytes();

    if (!reading_body) {
      if (used + bytes.size() > staging.size()) {
        (*received)->buffer.Release();
        failed = true;
        break;
      }
      std::memcpy(staging.data() + used, bytes.data(), bytes.size());
      used += bytes.size();
      (*received)->buffer.Release();
      received->reset();

      const auto header_end = coropact_bench::HeaderTerminatorEnd(
          reinterpret_cast<const char*>(staging.data()), used);
      if (header_end == static_cast<std::size_t>(-1)) {
        if (used == staging.size()) {
          failed = true;
          break;
        }
        continue;
      }

      const std::size_t content_length = coropact_bench::ParseContentLength(
          reinterpret_cast<const char*>(staging.data()), header_end);
      const std::size_t available = used - header_end;
      if (available >= content_length) {
        const std::size_t consumed = header_end + content_length;
        const std::size_t remain = used - consumed;
        auto written = co_await stream.WriteAll(response);
        if (!written.has_value()) {
          failed = true;
          break;
        }
        if (remain > 0) {
          std::memmove(staging.data(), staging.data() + consumed, remain);
        }
        used = remain;
        continue;
      }

      body_remain = content_length - available;
      used = 0;
      reading_body = true;
      continue;
    }

    if (bytes.size() >= body_remain) {
      const std::size_t leftover = bytes.size() - body_remain;
      if (leftover > staging.size()) {
        (*received)->buffer.Release();
        failed = true;
        break;
      }
      if (leftover > 0) {
        std::memcpy(staging.data(), bytes.data() + body_remain, leftover);
      }
      used = leftover;
      body_remain = 0;
      reading_body = false;
      (*received)->buffer.Release();
      received->reset();

      auto written = co_await stream.WriteAll(response);
      if (!written.has_value()) {
        failed = true;
        break;
      }
    } else {
      body_remain -= bytes.size();
      (*received)->buffer.Release();
      received->reset();
    }
  }

  (void)failed;
  (void)co_await source.Stop();
  (void)co_await stream.Close();
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  const auto port = static_cast<std::uint16_t>(coropact_bench::EnvInt("PORT", 19090));
  const std::size_t workers = coropact_bench::EnvSize("URING_WORKERS", 4);
  const auto entries = static_cast<std::uint32_t>(coropact_bench::EnvSize("URING_ENTRIES", 1024));
  const bool accept_multishot = AcceptMultishotEnabled();
  const bool zero_copy_writes = EnvEnabled("ZERO_COPY_WRITES", true);
  const bool use_recv_source = EnvEnabled("USE_RECV_SOURCE", true);
  const std::size_t shared_buffer_capacity =
      coropact_bench::EnvSize("SHARED_BUFFER_CAPACITY", 4096);
  const std::size_t shared_buffer_size =
      coropact_bench::EnvSize("SHARED_BUFFER_SIZE", coropact_bench::kRequestBufferSize);
  const std::size_t response_body = coropact_bench::ResponseBodySize();
  if (port == 0 || workers == 0) {
    return 2;
  }
  if (use_recv_source &&
      (shared_buffer_capacity == 0 || shared_buffer_capacity > 32 * 1024 ||
       (shared_buffer_capacity & (shared_buffer_capacity - 1)) != 0 || shared_buffer_size == 0)) {
    std::fprintf(stderr,
                 "SHARED_BUFFER_CAPACITY must be a power of two in [1, 32768]; "
                 "SHARED_BUFFER_SIZE must be > 0\n");
    return 2;
  }

  coropact::luring::detail::WorkerGroupOptions options;
  options.worker_num = workers;
  options.worker_options.loop_options.entries = entries;
  options.worker_options.loop_options.shared_buffer_capacity =
      use_recv_source ? shared_buffer_capacity : 0;
  options.worker_options.loop_options.shared_buffer_size = shared_buffer_size;
  options.worker_options.listen_options.reuse_addr = true;
  options.worker_options.listen_options.reuse_port = true;
  options.worker_options.listen_options.zero_copy_writes = zero_copy_writes;
  options.worker_options.accept_mode =
      accept_multishot ? coropact::luring::detail::AcceptMode::kMultishot
                       : coropact::luring::detail::AcceptMode::kSingleShot;

  coropact::luring::detail::WorkerGroup server(
      coropact::net::Endpoint::Loopback(port), std::move(options), {},
      [use_recv_source](coropact::luring::detail::WorkerContext&,
                        coropact::luring::Stream stream) {
        if (use_recv_source) {
          return HttpSessionRecvSource(std::move(stream));
        }
        return HttpSessionReadSome(std::move(stream));
      });
  auto started = server.Start();
  if (!started.has_value()) {
    std::fprintf(stderr, "WorkerGroup::Start failed: %s\n",
                 started.error().message().c_str());
    return 1;
  }

  std::printf(
      "HttpLuringBench bind=127.0.0.1 port=%u workers=%zu entries=%u accept=%s "
      "recv=%s zero_copy_writes=%s shared_buffer_capacity=%zu shared_buffer_size=%zu "
      "frame_pool=off response_body=%zu\n",
      port, workers, entries, accept_multishot ? "multishot" : "single-shot",
      use_recv_source ? "provided-buffer" : "read-some", zero_copy_writes ? "on" : "off",
      use_recv_source ? shared_buffer_capacity : 0, shared_buffer_size, response_body);
  std::fflush(stdout);
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  server.Stop();
  return 0;
}
