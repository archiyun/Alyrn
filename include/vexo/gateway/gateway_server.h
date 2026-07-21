// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "vexo/coro/scheduler.h"
#include "vexo/coro/spawn.h"
#include "vexo/coro/task.h"
#include "vexo/gateway/gateway_core.h"
#include "vexo/gateway/health_check_config.h"
#include "vexo/gateway/proxy_pass.h"
#include "vexo/gateway/upstream_conn_pool.h"
#include "vexo/gateway/upstream_registry.h"
#include "vexo/http/http_parser.h"
#include "vexo/io/async_listener.h"
#include "vexo/io/stream_algorithms.h"
#include "vexo/utils/macros.h"

namespace vexo::gateway {

// Coroutine gateway server over backend-neutral listener/stream concepts.
// It owns gateway routing/proxy orchestration and delegates actual IO to the
// supplied Listener and Connector implementations.
template <vexo::io::AsyncListener Listener, UpstreamConnector Connector>
class GatewayServer {
public:
  VEXO_DELETE_COPY_MOVE(GatewayServer);

  using Handler = GatewayCore::Handler;
  using Route = GatewayCore::Route;
  using RouteType = GatewayCore::RouteType;
  using MatchType = GatewayCore::MatchType;
  using Stream = typename Listener::Stream;
  using UpstreamStream = typename Connector::Stream;

  GatewayServer(Listener& listener, vexo::coro::Scheduler& scheduler, std::string name,
                UpstreamRegistry& registry, Connector& connector)
      : listener_(listener),
        scheduler_(scheduler),
        connector_(connector),
        core_(std::move(name), registry),
        registry_(registry) {}

  void set_thread_num(int /*num_threads*/) {}

  void Get(std::string_view path, Handler handler) { core_.Get(path, std::move(handler)); }
  void Post(std::string_view path, Handler handler) { core_.Post(path, std::move(handler)); }

  void AddProxyRoute(std::string_view path, std::string_view upstream_name,
                     std::string_view algo = "p2c") {
    core_.AddProxyRoute(path, upstream_name, algo);
  }

  void AddProxyRoute(std::string_view path, std::string_view upstream_name, FallbackConfig fallback,
                     bool circuit_breaker_enabled = false, std::string_view algo = "p2c") {
    core_.AddProxyRoute(path, upstream_name, std::move(fallback), circuit_breaker_enabled, algo);
  }

  void EnableHealthCheck(HealthCheckConfig cfg = {}) {
    health_check_cfg_ = std::move(cfg);
    health_check_enabled_ = true;
  }

  void EnableRateLimit(RateLimiterConfig cfg) { core_.EnableRateLimit(std::move(cfg)); }
  void EnableGlobalRateLimit(double rate, double burst) {
    core_.EnableGlobalRateLimit(rate, burst);
  }
  void EnablePerIPRateLimit(double rate, double burst) { core_.EnablePerIPRateLimit(rate, burst); }
  void set_pool_config(PoolConfig cfg) { pool_ = UpstreamStreamPool<UpstreamStream>(cfg); }

  const Route* MatchRoute(std::string_view path) const { return core_.MatchRoute(path); }

  void Start() {
    if (started_) return;
    started_ = true;
    vexo::coro::Spawn(scheduler_, AcceptLoop()).Detach();
    if (health_check_enabled_) {
      if constexpr (requires { connector_.SleepFor(std::chrono::milliseconds{1}); }) {
        vexo::coro::Spawn(scheduler_, HealthLoop()).Detach();
      }
    }
  }

  vexo::coro::Task<void> Stop() {
    stopping_ = true;
    co_await listener_.Close();
  }

private:
  static std::span<const std::byte> Bytes(std::string_view text) {
    return std::as_bytes(std::span<const char>(text.data(), text.size()));
  }

  static std::string ClientIp(const Stream& stream) {
    if constexpr (requires { stream.PeerAddress().ToIp(); }) {
      return stream.PeerAddress().ToIp();
    } else {
      return {};
    }
  }

  vexo::coro::Task<void> AcceptLoop() {
    for (;;) {
      auto accepted = co_await listener_.Accept();
      if (!accepted.has_value()) {
        co_return;
      }
      vexo::coro::Spawn(scheduler_, Session(std::move(*accepted))).Detach();
    }
  }

  vexo::coro::Task<void> Session(Stream stream) {
    vexo::http::HttpParser parser;
    std::array<std::byte, 4096> read_buffer{};
    const std::string client_ip = ClientIp(stream);

    for (;;) {
      auto read = co_await stream.ReadSome(read_buffer);
      if (!read.has_value() || *read == 0) break;

      std::string_view bytes(reinterpret_cast<const char*>(read_buffer.data()), *read);
      auto parse_status = parser.Feed(bytes);
      if (!co_await HandleParseStatus(stream, parse_status)) {
        co_await stream.Close();
        co_return;
      }

      while (parser.GotAll()) {
        vexo::http::HttpRequest req = parser.TakeRequest();
        auto action = core_.HandleRequest(req, client_ip);
        if (!co_await ApplyAction(stream, req, std::move(action))) {
          co_await stream.Close();
          co_return;
        }
        parse_status = parser.ParseAvailable();
        if (!co_await HandleParseStatus(stream, parse_status)) {
          co_await stream.Close();
          co_return;
        }
      }
    }

    co_await stream.Close();
  }

  vexo::coro::Task<bool> HandleParseStatus(Stream& stream, vexo::http::ParseStatus status) {
    if (status == vexo::http::ParseStatus::Continue || status == vexo::http::ParseStatus::GotAll) {
      co_return true;
    }

    auto action = core_.HandleParseError(status);
    co_await vexo::io::WriteAll(stream, Bytes(action.response));
    if (action.close_after_send) {
      co_await stream.Shutdown();
    }
    co_return false;
  }

  vexo::coro::Task<bool> ApplyAction(Stream& stream, const vexo::http::HttpRequest& req,
                                     GatewayCore::Action action) {
    if (action.kind == GatewayActionKind::Send) {
      auto written = co_await vexo::io::WriteAll(stream, Bytes(action.response));
      if (!written.has_value()) co_return false;
      if (action.close_after_send) {
        co_await stream.Shutdown();
        co_return false;
      }
      co_return true;
    }

    auto result = co_await ProxyPass::Forward(
        stream, req, *action.proxy.upstream, *action.proxy.load_balancer, pool_, connector_,
        action.proxy.request_ctx, action.proxy.circuit_breaker,
        core_.MakeForwardedContext(action.proxy));
    if (result.status == ProxyForwardStatus::kNoPeer) {
      auto fallback = core_.ProxyUnavailable(*action.proxy.route, "no available upstream peer");
      co_await vexo::io::WriteAll(stream, Bytes(fallback.response));
      co_return true;
    }
    if (result.status == ProxyForwardStatus::kClientClosed ||
        result.status == ProxyForwardStatus::kUpstreamFailed) {
      co_return false;
    }
    co_return true;
  }

  template <vexo::io::AsyncReadStream AnyStream>
  static vexo::coro::Task<vexo::base::Result<std::size_t>> ReadSomeForHealth(
      AnyStream& stream, std::span<std::byte> buffer, std::chrono::milliseconds timeout) {
    if constexpr (requires { stream.ReadSomeFor(buffer, timeout); }) {
      co_return co_await stream.ReadSomeFor(buffer, timeout);
    } else {
      co_return co_await stream.ReadSome(buffer);
    }
  }

  static int ParseHealthStatus(std::string_view headers) {
    const auto line_end = headers.find("\r\n");
    if (line_end == std::string_view::npos) return 0;

    const std::string_view line = headers.substr(0, line_end);
    if (!(line.starts_with("HTTP/1.1 ") || line.starts_with("HTTP/1.0 ")) || line.size() < 12) {
      return 0;
    }

    const std::string_view code = line.substr(9, 3);
    int status = 0;
    const auto [ptr, ec] = std::from_chars(code.data(), code.data() + code.size(), status);
    if (ec != std::errc{} || ptr != code.data() + code.size()) return 0;
    if (line.size() > 12 && line[12] != ' ') return 0;
    return status;
  }

  static uint64_t NowMs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());
  }

  vexo::coro::Task<void> HealthLoop() {
    const auto interval = std::chrono::milliseconds(
        static_cast<int>(std::max(health_check_cfg_.interval_sec, 0.001) * 1000.0));
    while (!stopping_) {
      co_await HealthCheckRound();
      co_await connector_.SleepFor(interval);
    }
  }

  vexo::coro::Task<void> HealthCheckRound() {
    for (const auto& [_, upstream] : registry_.all()) {
      for (const auto& peer : upstream->peers()) {
        const bool ok = co_await ProbePeer(*peer);
        CompleteHealthProbe(*peer, ok);
      }
    }
  }

  vexo::coro::Task<bool> ProbePeer(UpstreamPeer& peer) {
    auto connected = co_await connector_.Connect(peer.config().host, peer.config().port);
    if (!connected.has_value()) {
      co_return false;
    }

    auto stream = std::move(*connected);
    const std::string request = "GET " + health_check_cfg_.path +
                                " HTTP/1.1\r\n"
                                "Host: " +
                                peer.host_port() + "\r\nConnection: close\r\n\r\n";
    auto written = co_await vexo::io::WriteAll(stream, Bytes(request));
    if (!written.has_value()) {
      co_await stream.Close();
      co_return false;
    }

    std::array<std::byte, 4096> buffer{};
    std::string pending;
    const auto timeout = std::chrono::milliseconds(
        static_cast<int>(std::max(health_check_cfg_.timeout_sec, 0.001) * 1000.0));
    while (pending.size() <= 16 * 1024) {
      auto read = co_await ReadSomeForHealth(stream, buffer, timeout);
      if (!read.has_value() || *read == 0) {
        co_await stream.Close();
        co_return false;
      }
      pending.append(reinterpret_cast<const char*>(buffer.data()), *read);
      const auto header_end = pending.find("\r\n\r\n");
      if (header_end != std::string::npos) {
        const int status = ParseHealthStatus(std::string_view(pending.data(), header_end + 4));
        co_await stream.Close();
        const bool ok = status == 200;
        co_return ok;
      }
    }

    co_await stream.Close();
    co_return false;
  }

  void CompleteHealthProbe(UpstreamPeer& peer, bool success) {
    const std::string& name = peer.config().name;
    if (success) {
      peer.OnSuccess();
      health_failures_[name] = 0;
      int& ok = health_successes_[name];
      ++ok;
      if (ok >= health_check_cfg_.healthy_threshold) {
        peer.state().down.store(false, std::memory_order_relaxed);
        peer.state().fails.store(0, std::memory_order_relaxed);
        ok = 0;
      }
      return;
    }

    peer.OnFailure(NowMs());
    health_successes_[name] = 0;
    int& failures = health_failures_[name];
    ++failures;
    if (failures >= health_check_cfg_.unhealthy_threshold) {
      peer.state().down.store(true, std::memory_order_relaxed);
    }
  }

  Listener& listener_;
  vexo::coro::Scheduler& scheduler_;
  Connector& connector_;
  GatewayCore core_;
  UpstreamRegistry& registry_;
  UpstreamStreamPool<UpstreamStream> pool_;
  HealthCheckConfig health_check_cfg_{};
  std::unordered_map<std::string, int> health_successes_;
  std::unordered_map<std::string, int> health_failures_;
  bool started_{false};
  bool stopping_{false};
  bool health_check_enabled_{false};
};

}  // namespace vexo::gateway
