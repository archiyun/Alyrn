// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/gateway/health_check_config.h"
#include "coropact/io/async_connector.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/io/stream_algorithms.h"
#include "coropact/utils/macros.h"

namespace coropact::gateway {

// One active health loop per process (normally attached to worker 0). The
// connector is backend-specific, while the probe state machine is shared by
// Reactor and io_uring.
//
// Lifecycle:
//   1. construct after the registry is complete;
//   2. call Start() from a worker-init callback, passing that worker's
//      scheduler and connector;
//   3. call StopAndJoin() from a non-worker thread before stopping the worker.
//
// The last rule matters because a probe is scheduled on the worker's event
// loop. StopAndJoin waits for the coroutine to finish, so stopping the loop
// first would leave the join unresolved.
template <coropact::io::AsyncConnector Connector>
class GatewayHealthService {
public:
  COROPACT_DELETE_COPY_MOVE(GatewayHealthService);

  using Stream = typename Connector::Stream;

  GatewayHealthService(UpstreamRegistry& registry, HealthCheckConfig config = {})
      : registry_(registry), config_(std::move(config)) {}

  ~GatewayHealthService() { StopAndJoin(); }

  // Must be called once from the worker whose scheduler owns connector.
  [[nodiscard]]
  bool Start(coro::Scheduler& scheduler, Connector connector) {
    if (started_) return false;
    started_ = true;
    stopping_.store(false, std::memory_order_release);
    handle_.emplace(coro::Spawn(scheduler, Run(std::move(connector))));
    return true;
  }

  // Safe to call repeatedly. This function must not run on the worker that
  // owns the scheduler passed to Start().
  void StopAndJoin() noexcept {
    if (!started_) return;
    stopping_.store(true, std::memory_order_release);
    if (handle_) {
      handle_->Wait();
      handle_.reset();
    }
  }

  [[nodiscard]]
  bool Started() const noexcept {
    return started_;
  }

private:
  struct ProbeState {
    int consecutive_failures{0};
    int consecutive_successes{0};
  };

  static constexpr std::size_t kMaxResponseHeaderBytes = 16 * 1024;
  static constexpr auto kStopPollInterval = std::chrono::milliseconds(100);

  static std::chrono::milliseconds ToMilliseconds(double seconds) noexcept {
    if (!(seconds > 0.0)) return std::chrono::milliseconds(1);
    const double max_seconds =
        static_cast<double>(std::chrono::milliseconds::max().count()) / 1000.0;
    if (seconds >= max_seconds) return std::chrono::milliseconds::max();
    const auto count = static_cast<std::int64_t>(seconds * 1000.0);
    return std::chrono::milliseconds(std::max<std::int64_t>(1, count));
  }

  static std::span<const std::byte> Bytes(std::string_view text) noexcept {
    return std::as_bytes(std::span<const char>(text.data(), text.size()));
  }

  static bool IsSuccessfulResponse(std::string_view response) noexcept {
    const std::size_t line_end = response.find("\r\n");
    if (line_end == std::string_view::npos) return false;

    const std::string_view status_line = response.substr(0, line_end);
    if (status_line.size() < 12 || status_line.substr(0, 5) != "HTTP/") return false;

    const std::size_t space = status_line.find(' ');
    if (space == std::string_view::npos || space + 4 > status_line.size()) return false;
    int status = 0;
    const char* first = status_line.data() + space + 1;
    const char* last = first + 3;
    const auto parsed = std::from_chars(first, last, status);
    return parsed.ec == std::errc{} && parsed.ptr == last && status == 200;
  }

  template <class S>
  static constexpr bool HasTimedRead =
      requires(S& stream, std::span<std::byte> buffer, std::chrono::milliseconds timeout) {
        stream.ReadSomeFor(buffer, timeout);
      };

  coro::Task<bool> Probe(UpstreamPeer& peer, Connector& connector) {
    auto connected = co_await connector.Connect(peer.config().host, peer.config().port);
    if (!connected.has_value()) co_return false;

    Stream stream = std::move(*connected);
    const std::string request = "GET " + config_.path + " HTTP/1.1\r\nHost: " + peer.HostPort() +
                                "\r\nConnection: close\r\n\r\n";
    auto written = co_await io::WriteAll(stream, Bytes(request));
    if (!written.has_value()) {
      co_await stream.Close();
      co_return false;
    }

    std::array<std::byte, 4096> buffer{};
    std::string response;
    response.reserve(4096);
    const auto timeout = ToMilliseconds(config_.timeout_sec);

    while (response.find("\r\n\r\n") == std::string::npos) {
      if (response.size() >= kMaxResponseHeaderBytes) {
        co_await stream.Close();
        co_return false;
      }

      base::Result<std::size_t> read;
      if constexpr (HasTimedRead<Stream>) {
        read = co_await stream.ReadSomeFor(buffer, timeout);
      } else {
        // Existing Reactor/io_uring streams implement ReadSomeFor. Keep the
        // fallback for custom connectors, while making the limitation visible
        // instead of silently pretending a timeout exists.
        read = co_await stream.ReadSome(buffer);
      }
      if (!read.has_value() || *read == 0) {
        co_await stream.Close();
        co_return false;
      }

      const std::size_t room = kMaxResponseHeaderBytes - response.size();
      const std::size_t accepted = std::min({*read, std::min(buffer.size(), room)});
      if (accepted == 0) {
        co_await stream.Close();
        co_return false;
      }
      response.append(reinterpret_cast<const char*>(buffer.data()), accepted);
    }

    const bool healthy = IsSuccessfulResponse(response);
    co_await stream.Close();
    co_return healthy;
  }

  void ApplyResult(UpstreamPeer& peer, bool healthy) {
    ProbeState& state = probe_states_[&peer];
    if (healthy) {
      state.consecutive_failures = 0;
      if (++state.consecutive_successes >= config_.healthy_threshold) {
        peer.SetActiveHealthy();
        state.consecutive_successes = 0;
      }
      return;
    }

    state.consecutive_successes = 0;
    if (++state.consecutive_failures >= config_.unhealthy_threshold) {
      peer.SetActiveUnhealthy();
      state.consecutive_failures = 0;
    }
  }

  coro::Task<void> CheckRound(Connector& connector) {
    for (const auto& [_, upstream] : registry_.all()) {
      for (const auto& peer : upstream->peers()) {
        if (stopping_.load(std::memory_order_acquire)) co_return;
        ApplyResult(*peer, co_await Probe(*peer, connector));
      }
    }
  }

  coro::Task<void> SleepUntilNextRound(Connector& connector) {
    auto remaining = ToMilliseconds(config_.interval_sec);
    while (remaining > std::chrono::milliseconds::zero() &&
           !stopping_.load(std::memory_order_acquire)) {
      const auto slice = std::min(remaining, kStopPollInterval);
      co_await connector.SleepFor(slice);
      remaining -= slice;
    }
  }

  coro::Task<void> Run(Connector connector) {
    while (!stopping_.load(std::memory_order_acquire)) {
      co_await CheckRound(connector);
      co_await SleepUntilNextRound(connector);
    }
  }

  UpstreamRegistry& registry_;
  HealthCheckConfig config_;
  std::unordered_map<const UpstreamPeer*, ProbeState> probe_states_;
  std::atomic<bool> stopping_{false};
  bool started_{false};
  std::optional<coro::JoinHandle<void>> handle_;
};

}  // namespace coropact::gateway
